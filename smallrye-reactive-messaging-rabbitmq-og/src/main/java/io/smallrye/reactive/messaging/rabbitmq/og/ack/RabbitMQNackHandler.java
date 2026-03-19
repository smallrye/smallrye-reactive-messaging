package io.smallrye.reactive.messaging.rabbitmq.og.ack;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;

/**
 * Handler for message negative acknowledgement (nack).
 */
@FunctionalInterface
public interface RabbitMQNackHandler {

    /**
     * Handle negative acknowledgement of a message.
     *
     * @param message the message to nack
     * @param metadata additional nack metadata (may be null)
     * @param reason the reason for the nack
     * @param <V> message body type
     * @return a completion stage
     */
    <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Metadata metadata, Throwable reason);

}
