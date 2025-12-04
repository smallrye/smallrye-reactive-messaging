package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.rabbitmq.RabbitMQMessage;

/**
 * An implementation of {@link Message} suitable for incoming RabbitMQ messages.
 *
 * @param <T> the message body type
 */
public class IncomingRabbitMQMessage<T> implements ContextAwareMessage<T>, MetadataInjectableMessage<T> {

    /**
     * Used to ignore duplicate attempts to ack/nack because RabbitMQ considers this a connection level error.
     */
    private static class AlreadyAcknowledgedHandler implements RabbitMQAckHandler, RabbitMQFailureHandler {

        static final AlreadyAcknowledgedHandler INSTANCE = new AlreadyAcknowledgedHandler();

        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata, Context context,
                Throwable reason) {
            return CompletableFuture.completedFuture(null);
        }
    }

    public final io.vertx.rabbitmq.RabbitMQMessage message;
    protected Metadata metadata;
    protected final IncomingRabbitMQMetadata rabbitMQMetadata;
    private final ClientHolder holder;
    private final Context context;
    private final long deliveryTag;
    private RabbitMQFailureHandler onNack;
    private RabbitMQAckHandler onAck;
    private final String contentTypeOverride;
    private final T payload;

    public IncomingRabbitMQMessage(RabbitMQMessage delegate, ClientHolder holder,
            RabbitMQFailureHandler onNack,
            RabbitMQAckHandler onAck, String contentTypeOverride) {
        this(delegate.getDelegate(), holder, onNack, onAck, contentTypeOverride);
    }

    IncomingRabbitMQMessage(io.vertx.rabbitmq.RabbitMQMessage msg, ClientHolder holder,
            RabbitMQFailureHandler onNack, RabbitMQAckHandler onAck, String contentTypeOverride) {
        this.message = msg;
        this.deliveryTag = msg.envelope().getDeliveryTag();
        this.holder = holder;
        this.context = holder.getContext();
        this.contentTypeOverride = contentTypeOverride;
        this.rabbitMQMetadata = new IncomingRabbitMQMetadata(this.message);
        this.onNack = onNack;
        this.onAck = onAck;
        this.metadata = captureContextMetadata(rabbitMQMetadata);
        //noinspection unchecked
        this.payload = (T) convertPayload(message);
    }

    private IncomingRabbitMQMessage(io.vertx.rabbitmq.RabbitMQMessage message,
            IncomingRabbitMQMetadata rabbitMQMetadata, ClientHolder holder, Context context,
            long deliveryTag, String contentTypeOverride, T payload, RabbitMQAckHandler onAck,
            RabbitMQFailureHandler onNack, Metadata metadata) {
        this.message = message;
        this.rabbitMQMetadata = rabbitMQMetadata;
        this.holder = holder;
        this.context = context;
        this.deliveryTag = deliveryTag;
        this.contentTypeOverride = contentTypeOverride;
        this.payload = payload;
        this.onAck = onAck;
        this.onNack = onNack;
        this.metadata = metadata;
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return this::nack;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        try {
            // We must switch to the context having created the message.
            // This context is passed when this instance of message is created.
            // It's more a Vert.x RabbitMQ client issue which should ensure calling `accepted` on the right context.
            return onAck.handle(this, context);
        } finally {
            // Ensure ack/nack are only called once
            onAck = AlreadyAcknowledgedHandler.INSTANCE;
            onNack = AlreadyAcknowledgedHandler.INSTANCE;
        }
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        try {
            // We must switch to the context having created the message.
            // This context is passed when this instance of message is created.
            // It's more a Vert.x RabbitMQ client issue which should ensure calling `not accepted` on the right context.
            return onNack.handle(this, metadata, context, reason);
        } finally {
            // Ensure ack/nack are only called once
            onAck = AlreadyAcknowledgedHandler.INSTANCE;
            onNack = AlreadyAcknowledgedHandler.INSTANCE;
        }
    }

    @Override
    public <P> Message<P> withPayload(P payload) {
        return new IncomingRabbitMQMessage<>(message, rabbitMQMetadata, holder, context,
                deliveryTag, contentTypeOverride, payload, onAck, onNack, metadata);
    }

    /**
     * Acknowledges the message.
     */
    public void acknowledgeMessage() {
        holder.getAck(this.deliveryTag).subscribeAsCompletionStage();
    }

    /**
     * Rejects the message by nack'ing with requeue=false; this will either discard the message for good or
     * (if a DLQ has been set up) send it to the DLQ.
     *
     * @param reason the cause of the rejection, which must not be null
     */
    public void rejectMessage(Throwable reason) {
        this.rejectMessage(reason, false);
        holder.getNack(this.deliveryTag, false).apply(reason).subscribeAsCompletionStage();
    }

    /**
     * Rejects the message by nack'ing it.
     * <p>
     * This will either discard the message for good, requeue (if requeue=true is set)
     * or (if a DLQ has been set up) send it to the DLQ.
     * <p>
     * Please note that requeue is potentially dangerous as it can lead to
     * very high load if all consumers reject and requeue a message repeatedly.
     *
     * @param reason the cause of the rejection, which must not be null
     * @param requeue the requeue flag
     */
    public void rejectMessage(Throwable reason, boolean requeue) {
        holder.getNack(this.deliveryTag, requeue).apply(reason).subscribeAsCompletionStage();
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    private Object convertPayload(io.vertx.rabbitmq.RabbitMQMessage msg) {
        // Neither of these are guaranteed to be non-null
        String contentType = msg.properties().getContentType();
        final String contentEncoding = msg.properties().getContentEncoding();
        final Buffer body = msg.body();

        if (this.contentTypeOverride != null) {
            contentType = contentTypeOverride;
        }

        if (contentEncoding != null) {
            // Just silence the warning if we have a binary message
            if (!HttpHeaderValues.APPLICATION_OCTET_STREAM.toString().equalsIgnoreCase(contentType)) {
                log.typeConversionFallback();
            }
        }
        return body.getBytes();
    }

    public Map<String, Object> getHeaders() {
        return rabbitMQMetadata.getHeaders();
    }

    public Optional<String> getContentType() {
        return rabbitMQMetadata.getContentType();
    }

    public Optional<String> getEffectiveContentType() {
        return Optional.ofNullable(contentTypeOverride).or(() -> rabbitMQMetadata.getContentType());
    }

    public Optional<String> getContentEncoding() {
        return rabbitMQMetadata.getContentEncoding();
    }

    public Optional<Integer> getDeliveryMode() {
        return rabbitMQMetadata.getDeliveryMode();
    }

    public Optional<Integer> getPriority() {
        return rabbitMQMetadata.getPriority();
    }

    public Optional<String> getCorrelationId() {
        return rabbitMQMetadata.getCorrelationId();
    }

    public Optional<String> getReplyTo() {
        return rabbitMQMetadata.getReplyTo();
    }

    public Optional<String> getExpiration() {
        return rabbitMQMetadata.getExpiration();
    }

    public Optional<String> getMessageId() {
        return rabbitMQMetadata.getMessageId();
    }

    public Optional<ZonedDateTime> getTimestamp(final ZoneId zoneId) {
        return rabbitMQMetadata.getTimestamp(zoneId);
    }

    public Optional<String> getType() {
        return rabbitMQMetadata.getType();
    }

    public Optional<String> getUserId() {
        return rabbitMQMetadata.getUserId();
    }

    public Optional<String> getAppId() {
        return rabbitMQMetadata.getAppId();
    }

    /**
     * @deprecated Use getTimestamp()
     */
    @Deprecated
    public Optional<ZonedDateTime> getCreationTime(final ZoneId zoneId) {
        return rabbitMQMetadata.getTimestamp(zoneId);
    }

    public io.vertx.mutiny.rabbitmq.RabbitMQMessage getRabbitMQMessage() {
        return new io.vertx.mutiny.rabbitmq.RabbitMQMessage(message);
    }

    @Override
    public synchronized void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }

}
