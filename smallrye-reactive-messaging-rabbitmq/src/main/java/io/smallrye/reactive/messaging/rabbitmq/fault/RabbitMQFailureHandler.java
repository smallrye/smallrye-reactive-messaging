package io.smallrye.reactive.messaging.rabbitmq.fault;

import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.mutiny.core.Context;

/**
 * Implemented to provide message failure strategies.
 */
public interface RabbitMQFailureHandler {

    enum Strategy {
        /**
         * Mark the message as {@code rejected} and die.
         */
        FAIL,
        /**
         * Mark the message as {@code accepted} and continue.
         */
        ACCEPT,
        /**
         * Mark the message as {@code released} and continue.
         */
        RELEASE,
        /**
         * Mark the message as {@code rejected} and continue.
         */
        REJECT;

        public static Strategy from(String s) {
            if (s == null || s.equalsIgnoreCase("fail")) {
                return FAIL;
            }
            if (s.equalsIgnoreCase("accept")) {
                return ACCEPT;
            }
            if (s.equalsIgnoreCase("release")) {
                return RELEASE;
            }
            if (s.equalsIgnoreCase("reject")) {
                return REJECT;
            }
            throw ex.illegalArgumentUnknownFailureStrategy(s);
        }
    }

    /**
     * Handle message failure.
     * 
     * @param message the failed message
     * @param context the {@link Context} in which the handling should be done
     * @param reason the reason for the failure
     * @param <V> message body type
     * @return a {@link CompletionStage}
     */
    <V> CompletionStage<Void> handle(IncomingRabbitMQMessage<V> message, Context context, Throwable reason);

}
