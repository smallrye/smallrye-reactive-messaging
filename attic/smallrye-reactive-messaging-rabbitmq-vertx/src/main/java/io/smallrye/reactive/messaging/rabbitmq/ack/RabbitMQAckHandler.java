package io.smallrye.reactive.messaging.rabbitmq.ack;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.mutiny.core.Context;

/**
 * Implemented to provide message acknowledgement strategies.
 */
public interface RabbitMQAckHandler {

    enum Strategy {
        /**
         * Do no explicitly ack as it will be handled by RabbitMQ itself.
         */
        AUTO,
        /**
         * Explicit acks.
         */
        MANUAL;

    }

    /**
     * Handle the request to acknowledge a message.
     *
     * @param message the message to acknowledge
     * @param context the {@link Context} in which the acknowledgement should take place
     * @param <V> message body type
     * @return a {@link CompletionStage}
     */
    <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context);

}
