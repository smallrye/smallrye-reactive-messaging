package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.tracing.TracingUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Context;

/**
 * An implementation of {@link Message} suitable for incoming RabbitMQ messages.
 *
 * @param <T> the message body type
 */
public class IncomingRabbitMQMessage<T> implements ContextAwareMessage<T> {

    protected final io.vertx.rabbitmq.RabbitMQMessage message;
    protected Metadata metadata;
    protected final IncomingRabbitMQMetadata rabbitMQMetadata;
    private final ConnectionHolder holder;
    private final Context context;
    private final long deliveryTag;
    protected final RabbitMQFailureHandler onNack;
    protected final RabbitMQAckHandler onAck;

    IncomingRabbitMQMessage(io.vertx.mutiny.rabbitmq.RabbitMQMessage delegate, ConnectionHolder holder,
            boolean isTracingEnabled, RabbitMQFailureHandler onNack,
            RabbitMQAckHandler onAck) {
        this(delegate.getDelegate(), holder, isTracingEnabled, onNack, onAck);
    }

    IncomingRabbitMQMessage(io.vertx.rabbitmq.RabbitMQMessage msg, ConnectionHolder holder, boolean isTracingEnabled,
            RabbitMQFailureHandler onNack, RabbitMQAckHandler onAck) {
        this.message = msg;
        this.deliveryTag = msg.envelope().getDeliveryTag();
        this.holder = holder;
        this.context = holder.getContext();
        this.rabbitMQMetadata = new IncomingRabbitMQMetadata(this.message);
        this.onNack = onNack;
        this.onAck = onAck;
        this.metadata = captureContextMetadata(rabbitMQMetadata);

        // If tracing is enabled, ensure any tracing metadata in the received msg headers is transferred as metadata.
        if (isTracingEnabled) {
            this.metadata = this.metadata.with(TracingUtils.getTracingMetaData(msg));
        }
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
        // We must switch to the context having created the message.
        // This context is passed when this instance of message is created.
        // It's more a Vert.x RabbitMQ client issue which should ensure calling `accepted` on the right context.
        return onAck.handle(this, context);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        // We must switch to the context having created the message.
        // This context is passed when this instance of message is created.
        // It's more a Vert.x RabbitMQ client issue which should ensure calling `not accepted` on the right context.
        return onNack.handle(this, context, reason);
    }

    /**
     * Acknowledges the message.
     *
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

    @SuppressWarnings("unchecked")
    @Override
    public T getPayload() {
        // Throw a class cast exception if it cannot be converted.
        return (T) convertPayload(message);
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    private Object convertPayload(io.vertx.rabbitmq.RabbitMQMessage msg) {
        // Neither of these are guaranteed to be non-null
        final String contentType = msg.properties().getContentType();
        final String contentEncoding = msg.properties().getContentEncoding();
        final Buffer body = msg.body();

        // If there is a content encoding specified, we don't try to unwrap
        if (contentEncoding == null) {
            // Do our best with text and json
            if (HttpHeaderValues.APPLICATION_JSON.toString().equalsIgnoreCase(contentType)) {
                // This could be  JsonArray, JsonObject, String etc. depending on buffer contents
                return body.toJson();
            } else if (HttpHeaderValues.TEXT_PLAIN.toString().equalsIgnoreCase(contentType)) {
                return body.toString();
            }
        }

        // Otherwise fall back to raw byte array
        return body.getBytes();
    }

    public Optional<Integer> getPriority() {
        return rabbitMQMetadata.getPriority();
    }

    public Optional<String> getReplyTo() {
        return rabbitMQMetadata.getReplyTo();
    }

    public Optional<String> getUserId() {
        return rabbitMQMetadata.getUserId();
    }

    public Optional<String> getMessageId() {
        return rabbitMQMetadata.getId();
    }

    public Optional<ZonedDateTime> getCreationTime(final ZoneId zoneId) {
        return rabbitMQMetadata.getCreationTime(zoneId);
    }

    public Optional<String> getContentType() {
        return rabbitMQMetadata.getContentType();
    }

    public Optional<String> getCorrelationId() {
        return rabbitMQMetadata.getCorrelationId();
    }

    public Optional<String> getContentEncoding() {
        return rabbitMQMetadata.getContentEncoding();
    }

    public Map<String, Object> getHeaders() {
        return rabbitMQMetadata.getHeaders();
    }

    public io.vertx.mutiny.rabbitmq.RabbitMQMessage getRabbitMQMessage() {
        return new io.vertx.mutiny.rabbitmq.RabbitMQMessage(message);
    }

    public synchronized void injectTracingMetadata(TracingMetadata tracingMetadata) {
        metadata = metadata.with(tracingMetadata);
    }

}
