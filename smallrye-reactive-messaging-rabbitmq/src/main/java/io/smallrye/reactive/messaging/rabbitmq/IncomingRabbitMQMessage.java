package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

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

import io.netty.handler.codec.http.HttpHeaderValues;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mutiny.core.Context;

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

    protected final io.vertx.rabbitmq.RabbitMQMessage message;
    protected Metadata metadata;
    protected final IncomingRabbitMQMetadata rabbitMQMetadata;
    private final ConnectionHolder holder;
    private final Context context;
    private final long deliveryTag;
    private RabbitMQFailureHandler onNack;
    private RabbitMQAckHandler onAck;
    private final RabbitMQConnectorIncomingConfiguration incomingConfiguration;
    private final T payload;

    IncomingRabbitMQMessage(
            final io.vertx.mutiny.rabbitmq.RabbitMQMessage delegate,
            final ConnectionHolder holder,
            final RabbitMQFailureHandler onNack,
            final RabbitMQAckHandler onAck,
            final RabbitMQConnectorIncomingConfiguration incomingConfiguration) {
        this(delegate.getDelegate(), holder, onNack, onAck, incomingConfiguration);
    }

    IncomingRabbitMQMessage(
            final io.vertx.rabbitmq.RabbitMQMessage msg,
            final ConnectionHolder holder,
            final RabbitMQFailureHandler onNack,
            final RabbitMQAckHandler onAck,
            final RabbitMQConnectorIncomingConfiguration incomingConfiguration) {
        this.message = msg;
        this.deliveryTag = msg.envelope().getDeliveryTag();
        this.holder = holder;
        this.context = holder.getContext();
        this.rabbitMQMetadata = new IncomingRabbitMQMetadata(message, incomingConfiguration);
        this.onNack = onNack;
        this.onAck = onAck;
        this.incomingConfiguration = incomingConfiguration;
        this.metadata = captureContextMetadata(rabbitMQMetadata);
        //noinspection unchecked
        this.payload = (T) convertPayload(message);
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

    private Object convertPayload(io.vertx.rabbitmq.RabbitMQMessage msg) {
        // Neither of these are guaranteed to be non-null
        final String contentType = incomingConfiguration.getContentTypeOverride().orElse(msg.properties().getContentType());
        final String contentEncoding = msg.properties().getContentEncoding();
        final Buffer body = msg.body();

        // If there is a content encoding specified, we don't try to unwrap
        if (contentEncoding == null) {
            try {
                // Do our best with text and json
                if (HttpHeaderValues.APPLICATION_JSON.toString().equalsIgnoreCase(contentType)) {
                    // This could be  JsonArray, JsonObject, String etc. depending on buffer contents
                    return body.toJson();
                } else if (HttpHeaderValues.TEXT_PLAIN.toString().equalsIgnoreCase(contentType)) {
                    return body.toString();
                }
            } catch (Throwable t) {
                log.typeConversionFallback();
            }
            // Otherwise fall back to raw byte array
        } else {
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

}
