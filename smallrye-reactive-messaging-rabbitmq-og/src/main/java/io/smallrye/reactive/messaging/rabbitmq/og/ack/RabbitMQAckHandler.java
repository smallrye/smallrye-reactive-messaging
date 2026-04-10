package io.smallrye.reactive.messaging.rabbitmq.og.ack;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.og.IncomingRabbitMQMessage;

/**
 * Handler for message acknowledgement.
 */
@FunctionalInterface
public interface RabbitMQAckHandler {

    /**
     * Handle acknowledgement of a message.
     *
     * @param message the message to acknowledge
     * @param <V> message body type
     * @return a completion stage
     */
    <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message);

}
