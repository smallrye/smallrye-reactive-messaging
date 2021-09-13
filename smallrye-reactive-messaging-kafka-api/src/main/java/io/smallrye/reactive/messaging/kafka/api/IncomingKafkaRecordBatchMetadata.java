package io.smallrye.reactive.messaging.kafka.api;

import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * Contains information about the batch of messages received from a channel backed by Kafka.
 * Encapsulates underlying Kafka {@link ConsumerRecords} received from the consumer client.
 *
 * As this is an incoming message metadata it is created by the framework and injected into incoming batch messages.
 *
 * @param <K> The record key type
 * @param <T> The record payload type
 */
public class IncomingKafkaRecordBatchMetadata<K, T> {

    private final ConsumerRecords<K, T> records;

    public IncomingKafkaRecordBatchMetadata(ConsumerRecords<K, T> records) {
        this.records = records;
    }

    /**
     * @return the underlying Kafka {@link ConsumerRecords}
     */
    public ConsumerRecords<K, T> getRecords() {
        return records;
    }

    /**
     * @return the total number of records for all topic partitions
     */
    public int count() {
        return records.count();
    }

    /**
     * @return the set of topic partitions with data in this record batch
     */
    public Set<TopicPartition> partitions() {
        return records.partitions();
    }

}
