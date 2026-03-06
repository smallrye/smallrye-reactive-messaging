package io.smallrye.reactive.messaging.rabbitmq.og.ack;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;

/**
 * Auto acknowledgement handler for RabbitMQ messages.
 * Messages are automatically acknowledged by RabbitMQ when consumed (auto-ack mode).
 * This handler is a no-op since acknowledgement happens automatically.
 */
public class RabbitMQAutoAck implements RabbitMQAckHandler, RabbitMQNackHandler {

    public static final RabbitMQAutoAck INSTANCE = new RabbitMQAutoAck();

    private RabbitMQAutoAck() {
        // Singleton
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message) {
        // No-op - message is already acknowledged by RabbitMQ in auto-ack mode
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable reason) {
        // No-op - message is already acknowledged by RabbitMQ in auto-ack mode
        return CompletableFuture.completedFuture(null);
    }
}
