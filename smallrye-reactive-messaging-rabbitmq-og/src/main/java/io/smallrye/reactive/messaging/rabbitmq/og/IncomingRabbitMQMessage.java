package io.smallrye.reactive.messaging.rabbitmq.og;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
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

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return () -> ackHandler.handle(this);
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return (failure) -> nackHandler.handle(this, null, failure);
    }

    @Override
    public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return this::nack;
    }

    /**
     * Nack with metadata support.
     *
     * @param reason the reason for the nack
     * @param metadata additional nack metadata
     * @return a completion stage
     */
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return nackHandler.handle(this, metadata, reason);
    }

    @Override
    public synchronized void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }

    /**
     * Create a new message with a different payload but same metadata and ack/nack handlers.
     * This is useful for transforming messages in a processing chain.
     *
     * @param <P> the new payload type
     * @param newPayload the new payload
     * @return a new message with the new payload
     */
    public <P> Message<P> withPayload(P newPayload) {
        return new Message<P>() {
            @Override
            public P getPayload() {
                return newPayload;
            }

            @Override
            public Metadata getMetadata() {
                return IncomingRabbitMQMessage.this.metadata;
            }

            @Override
            public Supplier<CompletionStage<Void>> getAck() {
                return IncomingRabbitMQMessage.this.getAck();
            }

            @Override
            public Function<Throwable, CompletionStage<Void>> getNack() {
                return IncomingRabbitMQMessage.this.getNack();
            }

            @Override
            public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
                return IncomingRabbitMQMessage.this.getNackWithMetadata();
            }
        };
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
