package io.smallrye.reactive.messaging.amqp.fault;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.vertx.mutiny.core.Context;

public interface AmqpFailureHandler {

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

    <V> CompletionStage<Void> handle(AmqpMessage<V> message, Context context, Throwable reason);

}
