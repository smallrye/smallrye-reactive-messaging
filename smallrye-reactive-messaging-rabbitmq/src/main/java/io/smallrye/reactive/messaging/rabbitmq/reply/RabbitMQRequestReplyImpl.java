package io.smallrye.reactive.messaging.rabbitmq.reply;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.impl.Configs;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnectorCommonConfiguration;
import io.smallrye.reactive.messaging.rabbitmq.internals.RabbitMQClientHelper;

@Experimental("Experimental API")
public class RabbitMQRequestReplyImpl<Req, Rep> extends MutinyEmitterImpl<Req>
        implements RabbitMQRequestReply<Req, Rep>, MultiSubscriber<IncomingRabbitMQMessage<Rep>> {

    private static final String REPLY_TO = "amq.rabbitmq.reply-to";
    private final String channel;
    private final Duration replyTimeout;

    private final CorrelationIdHandler correlationIdHandler;
    private final ReplyFailureHandler replyFailureHandler;
    private Function<Message<Rep>, Uni<Message<Rep>>> replyConverter;

    private final Map<CorrelationId, PendingReplyImpl<Rep>> pendingReplies = new ConcurrentHashMap<>();
    private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();

    public RabbitMQRequestReplyImpl(EmitterConfiguration config,
            long defaultBufferSize, Config channelConfig,
            RabbitMQConnector connector, Instance<CorrelationIdHandler> correlationIdHandlers,
            Instance<ReplyFailureHandler> replyFailureHandlers) {
        super(config, defaultBufferSize);
        this.channel = config.name();
        Config connectorConfig = Configs.outgoing(channelConfig, RabbitMQConnector.CONNECTOR_NAME,
                channel);
        this.replyTimeout = connectorConfig.getOptionalValue(REPLY_TIMEOUT_KEY, Long.class)
                .map(Duration::ofMillis)
                .orElse(Duration.ofSeconds(5));
        String correlationIdHandlerIdentifier = connectorConfig
                .getOptionalValue(RabbitMQRequestReply.REPLY_CORRELATION_ID_HANDLER_KEY,
                        String.class)
                .orElse(RabbitMQRequestReply.DEFAULT_CORRELATION_ID_HANDLER);
        CorrelationIdHandler correlationIdHandler = CDIUtils.getInstanceById(correlationIdHandlers,
                correlationIdHandlerIdentifier).get();
        ReplyFailureHandler replyFailureHandler = connectorConfig
                .getOptionalValue(RabbitMQRequestReply.REPLY_FAILURE_HANDLER_KEY, String.class)
                .map(id -> CDIUtils.getInstanceById(replyFailureHandlers, id, () -> null))
                .orElse(null);
        this.correlationIdHandler = correlationIdHandler;
        this.replyFailureHandler = replyFailureHandler;

        Config incomingConfig = Configs.override(connectorConfig, Map.of(
                "shared-connection-name",
                RabbitMQClientHelper.resolveConnectionName(new RabbitMQConnectorCommonConfiguration(connectorConfig)),
                "auto-acknowledgement", true,
                "queue.declare", false,
                "queue.name", REPLY_TO));
        Publisher<IncomingRabbitMQMessage<Rep>> incomingChannel = (Publisher<IncomingRabbitMQMessage<Rep>>) connector
                .getPublisher(incomingConfig);
        incomingChannel.subscribe(this);
    }

    @Override
    public Flow.Publisher<Message<? extends Req>> getPublisher() {
        return this.publisher.onTermination().invoke(this::complete);
    }

    @Override
    public void complete() {
        super.complete();
        Subscriptions.cancel(subscription);
        for (CorrelationId correlationId : pendingReplies.keySet()) {
            PendingReplyImpl<Rep> reply = pendingReplies.remove(correlationId);
            if (reply != null) {
                reply.complete();
            }
        }
    }

    @Override
    public Uni<Rep> request(Req request) {
        return requestMulti(request).toUni();
    }

    @Override
    public Uni<Message<Rep>> request(Message<Req> request) {
        return requestMulti(request).toUni();
    }

    @Override
    public Multi<Rep> requestMulti(Req request) {
        return requestMulti(ContextAwareMessage.of(request))
                .map(Message::getPayload);
    }

    @Override
    public Multi<Message<Rep>> requestMulti(Message<Req> request) {
        var builder = request.getMetadata(OutgoingRabbitMQMetadata.class)
                .map(OutgoingRabbitMQMetadata::from)
                .orElseGet(OutgoingRabbitMQMetadata::builder);
        CorrelationId correlationId = correlationIdHandler.generate(request);
        builder.withCorrelationId(correlationId.toString()).withReplyTo(REPLY_TO);
        OutgoingRabbitMQMetadata outMetadata = builder.build();
        return sendMessage(request.addMetadata(outMetadata))
                .invoke(() -> subscription.get().request(1))
                .onItem()
                .transformToMulti(unused -> Multi.createFrom().<Message<Rep>> emitter(emitter -> {
                    pendingReplies.put(correlationId,
                            new PendingReplyImpl<>(outMetadata,
                                    (MultiEmitter<Message<Rep>>) emitter));
                }))
                .ifNoItem().after(replyTimeout)
                .failWith(() -> new RabbitMQRequestReplyTimeoutException(correlationId))
                .onItem().transformToUniAndConcatenate(m -> {
                    if (replyFailureHandler != null) {
                        Throwable failure = replyFailureHandler.handleReply(
                                (IncomingRabbitMQMessage<?>) m);
                        if (failure != null) {
                            return Uni.createFrom().failure(failure);
                        }
                    }
                    return Uni.createFrom().item(m);
                })
                .onTermination().invoke(() -> pendingReplies.remove(correlationId))
                .plug(multi -> replyConverter != null ? multi
                        .onItem().transformToUniAndConcatenate(f -> replyConverter.apply(f))
                        : multi);
    }

    public void setReplyConverter(Function<Message<Rep>, Uni<Message<Rep>>> converterFunction) {
        this.replyConverter = converterFunction;
    }

    @Override
    public Map<CorrelationId, PendingReply> getPendingReplies() {
        return new HashMap<>(pendingReplies);
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (Subscriptions.setIfEmpty(this.subscription, subscription)) {
            subscription.request(1);
        }
    }

    @Override
    public void onItem(IncomingRabbitMQMessage<Rep> item) {
        CorrelationId correlationId = item.getCorrelationId()
                .map(correlationIdHandler::parse)
                .orElse(null);
        if (correlationId != null) {
            PendingReplyImpl<Rep> reply = pendingReplies.get(correlationId);
            if (reply != null) {
                reply.emitter.emit(item);
            } else {
                log.requestReplyMessageIgnored(channel, correlationId.toString());
            }
        }
        // request more
        subscription.get().request(1);
    }

    @Override
    public void onFailure(Throwable failure) {
        log.requestReplyConsumerFailure(channel, failure);
    }

    @Override
    public void onCompletion() {

    }

    private static class PendingReplyImpl<Rep> implements PendingReply {

        private final OutgoingRabbitMQMetadata metadata;
        private final MultiEmitter<Message<Rep>> emitter;

        public PendingReplyImpl(OutgoingRabbitMQMetadata metadata,
                MultiEmitter<Message<Rep>> emitter) {
            this.metadata = metadata;
            this.emitter = emitter;
        }

        @Override
        public OutgoingRabbitMQMetadata metadata() {
            return metadata;
        }

        @Override
        public void complete() {
            emitter.complete();
        }

        @Override
        public boolean isCancelled() {
            return emitter.isCancelled();
        }

    }

}
