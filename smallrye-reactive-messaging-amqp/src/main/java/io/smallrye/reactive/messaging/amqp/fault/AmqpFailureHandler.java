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
        REJECT,

        /**
         * Mark the message as {@code modified} and continue.
         * This strategy increments the delivery count.
         * The message may be re-delivered on the same node.
         */
        MODIFIED_FAILED,

        /**
         * Mark the message as {@code modified} and continue.
         * This strategy increments the delivery count and mark the message as undeliverable on this node.
         * The message cannot be re-delivered on the same node, but may be redelivered on another node.
         */
        MODIFIED_FAILED_UNDELIVERABLE_HERE;

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
            if (s.equalsIgnoreCase("modified-failed")) {
                return MODIFIED_FAILED;
            }
            if (s.equalsIgnoreCase("modified-failed-undeliverable-here")) {
                return MODIFIED_FAILED_UNDELIVERABLE_HERE;
            }
            throw ex.illegalArgumentUnknownFailureStrategy(s);
        }
    }

    <V> CompletionStage<Void> handle(AmqpMessage<V> message, Context context, Throwable reason);

}
