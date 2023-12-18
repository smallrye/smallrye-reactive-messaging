package io.smallrye.reactive.messaging.kafka.reply;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;

/**
 * Handles failure cases in reply messages and extracts the throwable.
 * <p>
 * Typically, repliers can set a specific record header and send the reply in order for the ReplyFailureHandler
 * implementation to extract the error.
 * <p>
 * CDI-managed beans that implement this interface are discovered using
 * the {@link io.smallrye.common.annotation.Identifier} qualifier to be configured.
 *
 * @see KafkaRequestReply
 */
public interface ReplyFailureHandler {

    /**
     * Handles a reply received from Kafka to extract errors, if any.
     * Returned throwable will be used to fail the reply {@link io.smallrye.mutiny.Uni}.
     * If reply record contains no error, returns {@code null} in order for the record to be delivered.
     *
     * @param replyRecord the reply message
     * @return The throwable representing any error encountered during the reply.
     */
    Throwable handleReply(KafkaRecord<?, ?> replyRecord);
}
