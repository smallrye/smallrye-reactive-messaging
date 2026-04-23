package io.smallrye.reactive.messaging.amqp.reply;

import static io.smallrye.reactive.messaging.amqp.AmqpConnector.DIRECT_REPLY_TO_ADDRESS;
import static io.smallrye.reactive.messaging.amqp.AmqpConnector.ME_ADDRESS;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;

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
import io.smallrye.reactive.messaging.amqp.AmqpConnector;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;
import io.smallrye.reactive.messaging.providers.extension.MutinyEmitterImpl;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.smallrye.reactive.messaging.providers.impl.Configs;
import io.smallrye.reactive.messaging.providers.impl.OverrideConfig;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

@Experimental("Experimental API")
public class AmqpRequestReplyImpl<Req, Rep> extends MutinyEmitterImpl<Req>
        implements AmqpRequestReply<Req, Rep>, MultiSubscriber<AmqpMessage<Rep>> {

    private final String channel;
    private final AmqpConnector connector;
    private final String replyToAddress;
    private final Duration replyTimeout;

    private final CorrelationIdHandler correlationIdHandler;
    private final ReplyFailureHandler replyFailureHandler;
    private Function<Message<Rep>, Uni<Message<Rep>>> replyConverter;

    private final Map<CorrelationId, PendingReplyImpl<Rep>> pendingReplies = new ConcurrentHashMap<>();
    private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();

    @SuppressWarnings("unchecked")
    public AmqpRequestReplyImpl(EmitterConfiguration config,
            long defaultBufferSize, Config channelConfig,
            AmqpConnector connector, Instance<CorrelationIdHandler> correlationIdHandlers,
            Instance<ReplyFailureHandler> replyFailureHandlers) {
        super(config, defaultBufferSize);
        this.channel = config.name();
        this.connector = connector;
        Config connectorConfig = Configs.outgoing(channelConfig, AmqpConnector.CONNECTOR_NAME, channel);
        this.replyTimeout = connectorConfig.getOptionalValue(REPLY_TIMEOUT_KEY, Long.class)
                .map(Duration::ofMillis)
                .orElse(Duration.ofSeconds(5));
        String correlationIdHandlerIdentifier = connectorConfig
                .getOptionalValue(AmqpRequestReply.REPLY_CORRELATION_ID_HANDLER_KEY, String.class)
                .orElse(AmqpRequestReply.DEFAULT_CORRELATION_ID_HANDLER);
        this.correlationIdHandler = CDIUtils.getInstanceById(correlationIdHandlers,
                correlationIdHandlerIdentifier).get();
        this.replyFailureHandler = connectorConfig
                .getOptionalValue(AmqpRequestReply.REPLY_FAILURE_HANDLER_KEY, String.class)
                .map(id -> CDIUtils.getInstanceById(replyFailureHandlers, id, () -> null))
                .orElse(null);

        boolean linkPairing = connectorConfig.getOptionalValue(LINK_PAIRING_KEY, Boolean.class)
                .orElse(false);

        Map<String, Function<OverrideConfig, Object>> override = new HashMap<>();
        override.put("auto-acknowledgement", x -> true);
        connectorConfig.getOptionalValue("container-id", String.class)
                .ifPresent(s -> override.put("container-id", x -> s));

        if (linkPairing) {
            String linkName = connectorConfig.getOptionalValue("link-name", String.class).orElse(channel);
            boolean hasExplicitReplyAddress = connectorConfig.getOptionalValue(REPLY_ADDRESS_KEY, String.class)
                    .isPresent();
            this.replyToAddress = hasExplicitReplyAddress
                    ? connectorConfig.getOptionalValue(REPLY_ADDRESS_KEY, String.class).get()
                    : ME_ADDRESS;
            override.put("link-name", x -> linkName);
        } else {
            this.replyToAddress = connectorConfig.getOptionalValue(REPLY_ADDRESS_KEY, String.class)
                    .orElse(channel + "-reply");
        }
        override.put("address", x -> replyToAddress);

        Config incomingConfig = Configs.prefixOverride(connectorConfig, "reply", override);
        ((Publisher<AmqpMessage<Rep>>) connector.getPublisher(incomingConfig)).subscribe(this);
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
        var builder = request.getMetadata(OutgoingAmqpMetadata.class)
                .map(OutgoingAmqpMetadata::from)
                .orElseGet(OutgoingAmqpMetadata::builder);
        CorrelationId correlationId = correlationIdHandler.generate(request);
        // if using direct-reply-to reply-to address is assigned by broker when the receiver attaches
        String replyTo = DIRECT_REPLY_TO_ADDRESS.equals(replyToAddress) ? connector.getIncomingAddress(channel)
                : replyToAddress;
        builder.withMessageId(correlationId.toString()).withReplyTo(replyTo);
        OutgoingAmqpMetadata outMetadata = builder.build();
        // Register pending reply before sending to avoid race where a fast reply
        // arrives before the pending reply is registered and gets silently dropped
        return Multi.createFrom().<Message<Rep>> emitter(emitter -> {
            pendingReplies.put(correlationId,
                    new PendingReplyImpl<>(outMetadata,
                            (MultiEmitter<Message<Rep>>) emitter));
            sendMessage(request.addMetadata(outMetadata))
                    .subscribe().with(
                            unused -> subscription.get().request(1),
                            failure -> {
                                pendingReplies.remove(correlationId);
                                emitter.fail(failure);
                            });
        })
                .ifNoItem().after(replyTimeout)
                .failWith(() -> new AmqpRequestReplyTimeoutException(correlationId))
                .onItem().transformToUniAndConcatenate(m -> {
                    if (replyFailureHandler != null) {
                        Throwable failure = replyFailureHandler.handleReply(
                                (AmqpMessage<?>) m);
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
    public void onItem(AmqpMessage<Rep> item) {
        Object rawCorrelationId = item.getCorrelationId();

        CorrelationId correlationId = rawCorrelationId != null
                ? correlationIdHandler.parse(rawCorrelationId.toString())
                : null;
        if (correlationId != null) {
            PendingReplyImpl<Rep> reply = pendingReplies.get(correlationId);
            if (reply != null) {
                reply.emitter.emit(item);
            } else {
                log.requestReplyMessageIgnored(channel, correlationId.toString());
            }
        }
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

        private final OutgoingAmqpMetadata metadata;
        private final MultiEmitter<Message<Rep>> emitter;

        public PendingReplyImpl(OutgoingAmqpMetadata metadata,
                MultiEmitter<Message<Rep>> emitter) {
            this.metadata = metadata;
            this.emitter = emitter;
        }

        @Override
        public OutgoingAmqpMetadata metadata() {
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
