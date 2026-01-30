package io.smallrye.reactive.messaging.jms.fault;

import static io.smallrye.reactive.messaging.jms.i18n.JmsLogging.log;
import static io.smallrye.reactive.messaging.providers.wiring.Wiring.wireOutgoingConnectorToUpstream;

import java.util.Enumeration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.jms.*;
import io.smallrye.reactive.messaging.jms.impl.ImmutableJmsProperties;
import io.smallrye.reactive.messaging.providers.impl.Configs;

public class JmsDlqFailure implements JmsFailureHandler {

    public static final String CHANNEL_DLQ_SUFFIX = "dead-letter-queue";
    public static final String DEAD_LETTER_EXCEPTION_CLASS_NAME = "dead_letter_exception_class_name";
    public static final String DEAD_LETTER_CAUSE_CLASS_NAME = "dead_letter_cause_class_name";
    public static final String DEAD_LETTER_REASON = "dead_letter_reason";
    public static final String DEAD_LETTER_CAUSE = "dead_letter_cause";

    private final JmsConnectorIncomingConfiguration config;
    private final String dlqDestination;
    private final UnicastProcessor<Message<?>> dlqSource;

    @ApplicationScoped
    @Identifier(Strategy.DEAD_LETTER_QUEUE)
    public static class Factory implements JmsFailureHandler.Factory {
        @Inject
        Instance<SubscriberDecorator> subscriberDecorators;

        @Override
        public JmsFailureHandler create(JmsConnector connector, JmsConnectorIncomingConfiguration config,
                BiConsumer<Throwable, Boolean> reportFailure) {

            Optional<String> deadLetterQueueDestination = config.getDeadLetterQueueDestination();
            Config connectorConfig = Configs.prefixOverride(config.config(), CHANNEL_DLQ_SUFFIX,
                    Map.of("destination", c -> deadLetterQueueDestination.orElse("dead-letter-queue-" + config.getChannel())));

            JmsConnectorOutgoingConfiguration producerConfig = new JmsConnectorOutgoingConfiguration(connectorConfig);

            String deadQueueDestination = config.getDeadLetterQueueDestination()
                    .orElse("dead-letter-queue-" + config.getChannel());

            UnicastProcessor<Message<?>> processor = UnicastProcessor.create();
            Flow.Subscriber<? extends Message<?>> subscriber = connector.getSubscriber(connectorConfig);
            wireOutgoingConnectorToUpstream(processor, subscriber, subscriberDecorators,
                    producerConfig.getChannel() + "-" + CHANNEL_DLQ_SUFFIX);

            return new JmsDlqFailure(config, deadQueueDestination, processor);
        }
    }

    public JmsDlqFailure(JmsConnectorIncomingConfiguration config, String dlqDestination,
            UnicastProcessor<Message<?>> dlqSource) {
        this.config = config;
        this.dlqDestination = dlqDestination;
        this.dlqSource = dlqSource;
    }

    @Override
    public <T> Uni<Void> handle(IncomingJmsMessage<T> incomingMessage, Throwable reason, Metadata metadata) {
        OutgoingJmsMessageMetadata outgoingJmsMessageMetadata = getOutgoingJmsMessageMetadata(incomingMessage, reason);

        Message<T> dead = Message.of(incomingMessage.getPayload(), Metadata.of(outgoingJmsMessageMetadata));
        log.messageNackedDeadLetter(config.getChannel(), dlqDestination);
        CompletableFuture<Void> future = new CompletableFuture<>();
        dlqSource.onNext(dead
                .withAck(() -> dead.ack().thenAccept(__ -> future.complete(null)))
                .withNack(throwable -> {
                    future.completeExceptionally(throwable);
                    return future;
                }));
        return Uni.createFrom().completionStage(future)
                .emitOn(incomingMessage::runOnMessageContext);
    }

    private <T> OutgoingJmsMessageMetadata getOutgoingJmsMessageMetadata(IncomingJmsMessage<T> message, Throwable reason) {
        Optional<JmsProperties> optionalJmsProperties = message.getMetadata(IncomingJmsMessageMetadata.class)
                .map(JmsMessageMetadata::getProperties);
        JmsProperties jmsProperties = optionalJmsProperties
                .orElse(new ImmutableJmsProperties(message.unwrap(jakarta.jms.Message.class)));
        Enumeration<String> propertyNames = jmsProperties.getPropertyNames();
        JmsPropertiesBuilder jmsPropertiesBuilder = new JmsPropertiesBuilder();
        while (propertyNames.hasMoreElements()) {
            String propertyName = propertyNames.nextElement();
            Object propertyValue = jmsProperties.getObjectProperty(propertyName);
            jmsPropertiesBuilder.with(propertyName, propertyValue);
        }

        jmsPropertiesBuilder.with(DEAD_LETTER_EXCEPTION_CLASS_NAME, reason.getClass().getName());
        jmsPropertiesBuilder.with(DEAD_LETTER_REASON, getThrowableMessage(reason));
        if (reason.getCause() != null) {
            jmsPropertiesBuilder.with(DEAD_LETTER_CAUSE_CLASS_NAME, reason.getCause().getClass().getName());
            jmsPropertiesBuilder.with(DEAD_LETTER_CAUSE, getThrowableMessage(reason.getCause()));
        }

        return new OutgoingJmsMessageMetadata.OutgoingJmsMessageMetadataBuilder()
                .withProperties(jmsPropertiesBuilder.build())
                .build();
    }

    @Override
    public void close() {
        dlqSource.cancel();
    }

    private String getThrowableMessage(Throwable throwable) {
        String text = throwable.getMessage();
        if (text == null) {
            text = throwable.toString();
        }
        return text;
    }
}
