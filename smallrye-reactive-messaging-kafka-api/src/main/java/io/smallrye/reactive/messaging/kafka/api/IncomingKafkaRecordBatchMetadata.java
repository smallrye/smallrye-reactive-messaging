package io.smallrye.reactive.messaging.kafka.api;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

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
    private final int consumerGroupGenerationId;
    private final List<Message<?>> batchedMessages;

    public IncomingKafkaRecordBatchMetadata(ConsumerRecords<K, T> records, List<Message<?>> batchedMessages,
            String channel, int index, Map<TopicPartition, OffsetAndMetadata> offsets,
            int consumerGroupGenerationId) {
        this.records = records;
        this.batchedMessages = batchedMessages;
        this.channel = channel;
        this.index = index;
        this.offsets = Collections.unmodifiableMap(offsets);
        this.consumerGroupGenerationId = consumerGroupGenerationId;
    }

    public IncomingKafkaRecordBatchMetadata(ConsumerRecords<K, T> records, String channel, int index,
            Map<TopicPartition, OffsetAndMetadata> offsets, int consumerGroupGenerationId) {
        this(records, Collections.emptyList(), channel, index, offsets, consumerGroupGenerationId);
    }

    public IncomingKafkaRecordBatchMetadata(ConsumerRecords<K, T> records, String channel,
            Map<TopicPartition, OffsetAndMetadata> offsets) {
        this(records, channel, -1, offsets, -1);
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

    /**
     * @return the consumer group metadata generation id at the time of polling this record
     */
    public int getConsumerGroupGenerationId() {
        return consumerGroupGenerationId;
    }

    /**
     * @return batched messages
     */
    public List<Message<?>> getBatchedMessages() {
        return batchedMessages;
    }

    /**
     * Get metadata object for the given record.
     * This method is useful when you need to access metadata for a specific record in the batch.
     *
     * @param rec Kafka consumer record
     * @param metadata metadata type class
     * @return metadata object for the given record
     * @param <M> metadata type
     */
    public <M> M getMetadataForRecord(ConsumerRecord<K, T> rec, Class<M> metadata) {
        for (Message<?> record : batchedMessages) {
            if (record.getMetadata().get(IncomingKafkaRecordMetadata.class).orElseThrow().getRecord().equals(rec)) {
                return record.getMetadata(metadata).orElse(null);
            }
        }
        return null;
    }
}
