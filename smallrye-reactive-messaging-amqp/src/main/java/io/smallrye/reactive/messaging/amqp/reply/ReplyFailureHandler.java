package io.smallrye.reactive.messaging.amqp.reply;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;

/**
 * Handles failure cases in reply messages and extracts the throwable.
 * <p>
 * CDI-managed beans that implement this interface are discovered using
 * the {@link io.smallrye.common.annotation.Identifier} qualifier to be configured.
 *
 * @see AmqpRequestReply
 */
public interface ReplyFailureHandler {

    /**
     * Handles a reply received from AMQP to extract errors, if any.
     * Returned throwable will be used to fail the reply {@link io.smallrye.mutiny.Uni}.
     * If reply message contains no error, returns {@code null} in order for the message to be delivered.
     *
     * @param replyMessage the reply message
     * @return The throwable representing any error encountered during the reply.
     */
    Throwable handleReply(AmqpMessage<?> replyMessage);
}
