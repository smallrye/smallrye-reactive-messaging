package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;

import org.apache.kafka.common.header.Headers;

/**
 * Common interface for
 *
 * @param <K> the Kafka record key type
 */
public interface KafkaMessageMetadata<K> {

    /**
     * Get the topic
     *
     * @return the name of the topic
     */
    String getTopic();

    /**
     * Get the key
     *
     * @return the key
     */
    K getKey();

    /**
     * Get the timestamp
     *
     * @return the timestamp
     */
    Instant getTimestamp();

    /**
     * Get the Kafka headers
     *
     * @return the Kafka headers
     */
    Headers getHeaders();

    /**
     * Get the partition
     *
     * @return the partition
     */
    int getPartition();

}
