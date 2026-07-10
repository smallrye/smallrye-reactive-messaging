package io.smallrye.reactive.messaging.rabbitmq.og;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQAutoAck;
import io.smallrye.reactive.messaging.rabbitmq.og.ack.RabbitMQNackHandler;
import io.vertx.core.Context;
import io.vertx.core.internal.ContextInternal;

/**
 * Incoming RabbitMQ message implementation.
 * Wraps a RabbitMQ delivery and provides access to payload, metadata, and acknowledgement.
 */
public class IncomingRabbitMQMessage<T> implements ContextAwareMessage<T>, MetadataInjectableMessage<T> {

    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

    private final T payload;
    private Metadata metadata;
    private final RabbitMQAckHandler ackHandler;
    private final RabbitMQNackHandler nackHandler;
    private final IncomingRabbitMQMetadata rabbitMQMetadata;

    public IncomingRabbitMQMessage(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            Function<byte[], T> converter,
            RabbitMQAckHandler ackHandler,
            RabbitMQNackHandler nackHandler,
            Context vertxContext,
            String contentTypeOverride) {

        this.rabbitMQMetadata = new IncomingRabbitMQMetadata(envelope, properties, body, contentTypeOverride);
        this.payload = converter.apply(body);
        this.ackHandler = ackHandler;
        this.nackHandler = nackHandler;

        // Create a duplicated context for per-message context propagation.
        // This is needed because messages are created on the RabbitMQ consumer thread
        // (not a Vert.x thread), so captureContextMetadata() would return null.
        if (vertxContext instanceof ContextInternal) {
            Context duplicated = ((ContextInternal) vertxContext).duplicate();
            this.metadata = Metadata.of(rabbitMQMetadata, new LocalContextMetadata(duplicated));
        } else {
            this.metadata = Metadata.of(rabbitMQMetadata);
        }
    }

    /**
     * Create a message with auto-ack (no explicit acknowledgement needed).
     */
    public static <T> IncomingRabbitMQMessage<T> create(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            Function<byte[], T> converter) {
        return create(envelope, properties, body, converter, (Context) null, null);
    }

    /**
     * Create a message with auto-ack and a Vert.x context for context propagation.
     */
    public static <T> IncomingRabbitMQMessage<T> create(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            Function<byte[], T> converter,
            Context vertxContext,
            String contentTypeOverride) {
        return new IncomingRabbitMQMessage<>(
                envelope,
                properties,
                body,
                converter,
                RabbitMQAutoAck.INSTANCE,
                RabbitMQAutoAck.INSTANCE,
                vertxContext,
                contentTypeOverride);
    }

    /**
     * Create a message with manual acknowledgement.
     */
    public static <T> IncomingRabbitMQMessage<T> create(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            Function<byte[], T> converter,
            RabbitMQAckHandler ackHandler,
            RabbitMQNackHandler nackHandler) {
        return create(envelope, properties, body, converter, ackHandler, nackHandler, null, null);
    }

    /**
     * Create a message with manual acknowledgement and a Vert.x context for context propagation.
     */
    public static <T> IncomingRabbitMQMessage<T> create(
            Envelope envelope,
            AMQP.BasicProperties properties,
            byte[] body,
            Function<byte[], T> converter,
            RabbitMQAckHandler ackHandler,
            RabbitMQNackHandler nackHandler,
            Context vertxContext,
            String contentTypeOverride) {
        return new IncomingRabbitMQMessage<>(envelope, properties, body, converter, ackHandler, nackHandler,
                vertxContext, contentTypeOverride);
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Get the RabbitMQ-specific metadata.
     */
    public IncomingRabbitMQMetadata getRabbitMQMetadata() {
        return rabbitMQMetadata;
    }

    public java.util.Optional<String> getCorrelationId() {
        return java.util.Optional.ofNullable(rabbitMQMetadata.getCorrelationId());
    }

    public java.util.Map<String, Object> getHeaders() {
        return rabbitMQMetadata.getHeaders();
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
        if (acknowledged.compareAndSet(false, true)) {
            return ackHandler.handle(this)
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            acknowledged.set(false);
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        if (acknowledged.compareAndSet(false, true)) {
            return nackHandler.handle(this, metadata, reason)
                    .whenComplete((v, t) -> {
                        if (t != null) {
                            acknowledged.set(false);
                        }
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }

    /**
     * Default converter from byte array to String using UTF-8.
     */
    public static final Function<byte[], String> STRING_CONVERTER = body -> new String(body, StandardCharsets.UTF_8);

    /**
     * Converter that returns the raw byte array.
     */
    public static final Function<byte[], byte[]> BYTE_ARRAY_CONVERTER = body -> body;
}
