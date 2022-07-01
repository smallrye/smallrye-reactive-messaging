package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.tracing.TracingUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Context;
import io.vertx.rabbitmq.RabbitMQMessage;

/**
 * An implementation of {@link Message} suitable for incoming RabbitMQ messages.
 *
 * @param <T> the message body type
 */
public class IncomingRabbitMQMessage<T> implements ContextAwareMessage<T> {

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
        public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context, Throwable reason) {
            return CompletableFuture.completedFuture(null);
        }
    }

    protected final RabbitMQMessage message;
    protected Metadata metadata;
    protected final IncomingRabbitMQMetadata rabbitMQMetadata;
    private final ConnectionHolder holder;
    private final Context context;
    private final long deliveryTag;
    private RabbitMQFailureHandler onNack;
    private RabbitMQAckHandler onAck;
    private final T payload;

    static IncomingRabbitMQMessage<Buffer> create(io.vertx.mutiny.rabbitmq.RabbitMQMessage msg, ConnectionHolder holder,
            boolean isTracingEnabled,
            RabbitMQFailureHandler onNack, RabbitMQAckHandler onAck) {
        return create(msg.getDelegate(), holder, isTracingEnabled, onNack, onAck, msg.getDelegate().body());
    }

    static <T> IncomingRabbitMQMessage<T> create(io.vertx.rabbitmq.RabbitMQMessage msg, ConnectionHolder holder,
            boolean isTracingEnabled,
            RabbitMQFailureHandler onNack, RabbitMQAckHandler onAck, T payload) {

        IncomingRabbitMQMetadata rabbitMQMetadata = new IncomingRabbitMQMetadata(msg);
        Metadata metadata = captureContextMetadata(rabbitMQMetadata);

        // If tracing is enabled, ensure any tracing metadata in the received msg headers is transferred as metadata.
        if (isTracingEnabled) {
            metadata = metadata.with(TracingUtils.getTracingMetaData(msg));
        }

        return new IncomingRabbitMQMessage<>(msg, metadata, rabbitMQMetadata, holder, holder.getContext(),
                msg.envelope().getDeliveryTag(), onNack, onAck, payload);
    }

    private IncomingRabbitMQMessage(RabbitMQMessage message, Metadata metadata,
            IncomingRabbitMQMetadata rabbitMQMetadata,
            ConnectionHolder holder, Context context, long deliveryTag,
            RabbitMQFailureHandler onNack, RabbitMQAckHandler onAck, T payload) {
        this.message = message;
        this.metadata = metadata;
        this.rabbitMQMetadata = rabbitMQMetadata;
        this.holder = holder;
        this.context = context;
        this.deliveryTag = deliveryTag;
        this.onNack = onNack;
        this.onAck = onAck;
        this.payload = payload;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
        //return () -> onAck.handle(this, context);
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }

    @Override
    public CompletionStage<Void> ack() {
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
            return onNack.handle(this, context, reason);
        } finally {
            // Ensure ack/nack are only called once
            onAck = AlreadyAcknowledgedHandler.INSTANCE;
            onNack = AlreadyAcknowledgedHandler.INSTANCE;
        }
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
        holder.getNack(this.deliveryTag, false).apply(reason);
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    public Map<String, Object> getHeaders() {
        return rabbitMQMetadata.getHeaders();
    }

    public Optional<String> getContentType() {
        return rabbitMQMetadata.getContentType();
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

    public synchronized void injectTracingMetadata(TracingMetadata tracingMetadata) {
        metadata = metadata.with(tracingMetadata);
    }

    public <U> IncomingRabbitMQMessage<U> withPayload(U payload) {
        return new IncomingRabbitMQMessage<>(message, metadata, rabbitMQMetadata, holder, context, deliveryTag, onNack, onAck,
                payload);
    }

}
