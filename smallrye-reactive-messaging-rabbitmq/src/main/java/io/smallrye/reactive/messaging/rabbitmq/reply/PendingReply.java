package io.smallrye.reactive.messaging.rabbitmq.reply;

import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;

/**
 * A pending reply for a request.
 */
public interface PendingReply {

    /**
     * @return the metadata of the request
     */
    OutgoingRabbitMQMetadata metadata();

    /**
     * Complete the pending reply.
     */
    void complete();

    /**
     * @return whether the pending reply was terminated (with a completion or failure).
     */
    boolean isCancelled();
}
