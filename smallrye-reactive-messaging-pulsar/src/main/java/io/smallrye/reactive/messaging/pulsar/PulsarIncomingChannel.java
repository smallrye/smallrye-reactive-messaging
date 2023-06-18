package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.KeySharedPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.messaging.MessagingSpanNameExtractor;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.providers.locals.ContextOperator;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarAttributesExtractor;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarTrace;
import io.smallrye.reactive.messaging.pulsar.tracing.PulsarTraceTextMapGetter;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Vertx;

public class PulsarIncomingChannel<T> {

    private final Consumer<T> consumer;
    private final Flow.Publisher<? extends Message<?>> publisher;
    private final String channel;
    private final PulsarAckHandler ackHandler;
    private final PulsarFailureHandler failureHandler;
    private final EventLoopContext context;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<Throwable> failures = new ArrayList<>();

    private final boolean healthEnabled;

    private final boolean tracingEnabled;

    private final Instrumenter<PulsarTrace, Void> instrumenter;

    public PulsarIncomingChannel(PulsarClient client, Vertx vertx, Schema<T> schema,
            PulsarAckHandler.Factory ackHandlerFactory,
            PulsarFailureHandler.Factory failureHandlerFactory,
            PulsarConnectorIncomingConfiguration ic,
            ConfigResolver configResolver) throws PulsarClientException {
        this.channel = ic.getChannel();
        this.healthEnabled = ic.getHealthEnabled();
        this.tracingEnabled = ic.getTracingEnabled();
        ConsumerBuilder<T> builder = client.newConsumer(schema);
        ConsumerConfigurationData<?> conf = configResolver.getConsumerConf(ic);
        if (conf.getSubscriptionName() == null) {
            String s = UUID.randomUUID().toString();
            log.noSubscriptionName(s);
            conf.setSubscriptionName(s);
        }
        if (hasTopicConfig(conf)) {
            conf.setTopicNames(Arrays.stream(ic.getTopic().orElse(channel).split(",")).collect(Collectors.toSet()));
        }
        if (conf.getConsumerName() == null) {
            conf.setConsumerName(channel);
        }
        builder.loadConf(configResolver.configToMap(conf));
        ic.getDeadLetterPolicyMaxRedeliverCount().ifPresent(i -> builder.deadLetterPolicy(getDeadLetterPolicy(ic, i)));
        ic.getNegativeAckRedeliveryBackoff().ifPresent(s -> builder.negativeAckRedeliveryBackoff(parseBackoff(s)));
        ic.getAckTimeoutRedeliveryBackoff().ifPresent(s -> builder.ackTimeoutRedeliveryBackoff(parseBackoff(s)));
        if (conf.getConsumerEventListener() != null) {
            builder.consumerEventListener(conf.getConsumerEventListener());
        }
        if (conf.getPayloadProcessor() != null) {
            builder.messagePayloadProcessor(conf.getPayloadProcessor());
        }
        if (conf.getKeySharedPolicy() != null) {
            builder.keySharedPolicy(conf.getKeySharedPolicy());
        } else if (conf.getSubscriptionType() == SubscriptionType.Key_Shared) {
            builder.keySharedPolicy(KeySharedPolicy.autoSplitHashRange());
        }
        if (conf.getCryptoKeyReader() != null) {
            builder.cryptoKeyReader(conf.getCryptoKeyReader());
        }
        if (conf.getMessageCrypto() != null) {
            builder.messageCrypto(conf.getMessageCrypto());
        }
        if (ic.getBatchReceive() && conf.getBatchReceivePolicy() == null) {
            builder.batchReceivePolicy(BatchReceivePolicy.DEFAULT_POLICY);
        }

        this.consumer = builder.subscribe();
        log.createdConsumerWithConfig(channel, SchemaResolver.getSchemaName(schema), conf);
        this.ackHandler = ackHandlerFactory.create(consumer, ic);
        this.failureHandler = failureHandlerFactory.create(consumer, ic, this::reportFailure);
        this.context = ((VertxInternal) vertx.getDelegate()).createEventLoopContext();
        if (!ic.getBatchReceive()) {
            Multi<PulsarIncomingMessage<T>> receiveMulti = Multi.createBy().repeating()
                    .completionStage(consumer::receiveAsync)
                    .until(m -> closed.get())
                    .plug(msgMulti -> {
                        // Calling getValue on the pulsar-client-internal thread to make sure the SchemaInfo is fetched
                        if (schema instanceof AutoConsumeSchema || schema instanceof KeyValueSchema) {
                            return msgMulti.onItem().call(msg -> Uni.createFrom().item(msg::getValue));
                        } else {
                            return msgMulti;
                        }
                    })
                    .emitOn(command -> context.runOnContext(event -> command.run()))
                    .onItem().transform(message -> new PulsarIncomingMessage<>(message, ackHandler, failureHandler))
                    .onFailure(throwable -> isEndOfStream(client, throwable)).recoverWithCompletion()
                    .onFailure().invoke(failure -> {
                        log.failedToReceiveFromConsumer(channel, failure);
                        reportFailure(failure, false);
                    });
            if (tracingEnabled) {
                receiveMulti = receiveMulti.onItem().invoke(this::incomingTrace);
            }
            this.publisher = receiveMulti
                    .emitOn(context.nettyEventLoop())
                    .plug(ContextOperator::apply);
        } else {
            Multi<PulsarIncomingBatchMessage<T>> batchReceiveMulti = Multi.createBy().repeating()
                    .completionStage(consumer::batchReceiveAsync)
                    .until(m -> closed.get())
                    .filter(m -> m.size() > 0)
                    .plug(msgMulti -> {
                        // Calling getValue on the pulsar-client-internal thread to make sure the SchemaInfo is fetched
                        if (schema instanceof AutoConsumeSchema || schema instanceof KeyValueSchema) {
                            return msgMulti.onItem().call(msg -> Uni.createFrom().item(() -> {
                                msg.forEach(m -> m.getValue());
                                return null;
                            }));
                        } else {
                            return msgMulti;
                        }
                    })
                    .emitOn(command -> context.runOnContext(event -> command.run()))
                    .onItem().transform(m -> new PulsarIncomingBatchMessage<>(m, ackHandler, failureHandler))
                    .onFailure(throwable -> isEndOfStream(client, throwable)).recoverWithCompletion()
                    .onFailure().invoke(failure -> {
                        log.failedToReceiveFromConsumer(channel, failure);
                        reportFailure(failure, false);
                    });
            if (tracingEnabled) {
                batchReceiveMulti = batchReceiveMulti.onItem().invoke(this::incomingBatchTrace);
            }
            this.publisher = batchReceiveMulti
                    .emitOn(context.nettyEventLoop())
                    .plug(ContextOperator::apply);
        }

        PulsarAttributesExtractor attributesExtractor = new PulsarAttributesExtractor();
        MessagingAttributesGetter<PulsarTrace, Void> messagingAttributesGetter = attributesExtractor
                .getMessagingAttributesGetter();
        InstrumenterBuilder<PulsarTrace, Void> instrumenterBuilder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging",
                MessagingSpanNameExtractor.create(messagingAttributesGetter, MessageOperation.RECEIVE));

        instrumenter = instrumenterBuilder
                .addAttributesExtractor(
                        MessagingAttributesExtractor.create(messagingAttributesGetter, MessageOperation.RECEIVE))
                .addAttributesExtractor(attributesExtractor)
                .buildConsumerInstrumenter(PulsarTraceTextMapGetter.INSTANCE);
    }

    public void incomingTrace(PulsarMessage<T> pulsarMessage) {
        PulsarIncomingMessageMetadata metadata = pulsarMessage.getMetadata(PulsarIncomingMessageMetadata.class).get();
        TracingUtils.traceIncoming(instrumenter, pulsarMessage, new PulsarTrace.Builder()
                .withConsumerName(consumer.getConsumerName())
                .withMessage(metadata.getMessage())
                .build());
    }

    public void incomingBatchTrace(PulsarIncomingBatchMessage<T> pulsarMessage) {
        for (PulsarMessage<T> message : pulsarMessage.getMessages()) {
            incomingTrace(message);
        }
    }

    private boolean isEndOfStream(PulsarClient client, Throwable throwable) {
        if (closed.get()) {
            return true;
        } else if (consumer.hasReachedEndOfTopic()) {
            log.consumerReachedEndOfTopic(channel);
            return true;
        } else if (client.isClosed()) {
            log.clientClosed(channel, throwable);
            return true;
        }
        return false;
    }

    private static DeadLetterPolicy getDeadLetterPolicy(PulsarConnectorIncomingConfiguration ic, Integer redeliverCount) {
        return DeadLetterPolicy.builder()
                .maxRedeliverCount(redeliverCount)
                .deadLetterTopic(ic.getDeadLetterPolicyDeadLetterTopic().orElse(null))
                .retryLetterTopic(ic.getDeadLetterPolicyRetryLetterTopic().orElse(null))
                .initialSubscriptionName(ic.getDeadLetterPolicyInitialSubscriptionName().orElse(null))
                .build();
    }

    private RedeliveryBackoff parseBackoff(String backoffString) {
        String[] strings = backoffString.split(",");
        try {
            return MultiplierRedeliveryBackoff.builder()
                    .minDelayMs(Long.parseLong(strings[0]))
                    .maxDelayMs(Long.parseLong(strings[1]))
                    .multiplier(Double.parseDouble(strings[2]))
                    .build();
        } catch (Exception e) {
            log.unableToParseRedeliveryBackoff(backoffString, this.channel);
            return null;
        }
    }

    private static boolean hasTopicConfig(ConsumerConfigurationData<?> conf) {
        return conf.getTopicsPattern() != null
                || (conf.getTopicNames() != null && conf.getTopicNames().isEmpty());
    }

    public Flow.Publisher<? extends Message<?>> getPublisher() {
        return publisher;
    }

    public String getChannel() {
        return channel;
    }

    public Consumer<T> getConsumer() {
        return consumer;
    }

    public void close() {
        closed.set(true);
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            log.unableToCloseConsumer(e);
        }
    }

    public synchronized void reportFailure(Throwable failure, boolean fatal) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);

        if (fatal) {
            close();
        }
    }

    public void isStarted(HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            builder.add(channel, consumer.isConnected());
        }
    }

    public void isReady(HealthReport.HealthReportBuilder builder) {
        isStarted(builder);
    }

    public void isAlive(HealthReport.HealthReportBuilder builder) {
        if (healthEnabled) {
            List<Throwable> actualFailures;
            synchronized (this) {
                actualFailures = new ArrayList<>(failures);
            }
            if (!actualFailures.isEmpty()) {
                builder.add(channel, false,
                        actualFailures.stream().map(Throwable::getMessage).collect(Collectors.joining()));
            } else {
                builder.add(channel, true);
            }
        }

    }
}
