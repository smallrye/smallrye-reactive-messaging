package io.smallrye.reactive.messaging.kafka.api;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
    private final String channel;
    private final int index;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public IncomingKafkaRecordBatchMetadata(ConsumerRecords<K, T> records, String channel, int index,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.records = records;
        this.channel = channel;
        this.index = index;
        this.offsets = Collections.unmodifiableMap(offsets);
    }

    public IncomingKafkaRecordBatchMetadata(ConsumerRecords<K, T> records, String channel,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        this(records, channel, -1, offsets);
    }

    /**
     * @return channel name from which this message is consumed
     */
    public String getChannel() {
        return channel;
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

    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return offsets;
    }

    public int getConsumerIndex() {
        return index;
    }
}
