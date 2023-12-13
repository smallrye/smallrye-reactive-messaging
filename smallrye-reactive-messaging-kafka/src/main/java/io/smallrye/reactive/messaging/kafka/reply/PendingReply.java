package io.smallrye.reactive.messaging.kafka.reply;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A pending reply for a request.
 */
public interface PendingReply {

    /**
     * @return reply topic
     */
    String replyTopic();

    /**
     * @return reply partition, -1 if not set
     */
    int replyPartition();

    /**
     * @return the recordMetadata of the request
     */
    RecordMetadata recordMetadata();
}
