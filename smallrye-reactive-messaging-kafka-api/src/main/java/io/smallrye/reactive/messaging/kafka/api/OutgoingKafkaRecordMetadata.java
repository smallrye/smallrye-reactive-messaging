package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Contains information the user can set to influence how messages are sent via Kafka.
 *
 * @param <K> The Kafka record key type
 */
public class OutgoingKafkaRecordMetadata<K> implements KafkaMessageMetadata<K> {

    private final String topic;
    private final K recordKey;
    private final int partition;
    private final Instant timestamp;
    private final Headers headers;

    /**
     * Gets a {@link OutgoingKafkaRecordMetadataBuilder} that can be used to construct
     * instances of {@code OutgoingKafkaRecordMetadata}
     *
     * @param <K> the Kafka record key type
     * @return a new builder
     */
    public static <K> OutgoingKafkaRecordMetadataBuilder<K> builder() {
        return new OutgoingKafkaRecordMetadataBuilder<>();
    }

    protected OutgoingKafkaRecordMetadata(String topic, K key, int partition, Instant timestamp,
            Headers headers) {
        this.topic = topic;
        this.recordKey = key;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPartition() {
        return partition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Headers getHeaders() {
        return headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTopic() {
        return topic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getKey() {
        return recordKey;
    }

    /**
     * Builder to create {@link OutgoingKafkaRecordMetadata} instances
     *
     * @param <K> the Kafka record key type
     */
    public static class OutgoingKafkaRecordMetadataBuilder<K> {
        private String topic;
        private K recordKey;
        private int partition = -1;
        private Instant timestamp = null;
        private Headers headers;

        /**
         * By default the hardcoded topic in the configuration is used. If decisions need to be made dynamically about
         * which topic to use, it may be specified here.
         *
         * @param topic the topic name
         * @return this builder
         */
        public OutgoingKafkaRecordMetadataBuilder<K> withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Set the Kafka record key
         *
         * @param recordKey the key
         * @return this builder
         */
        public OutgoingKafkaRecordMetadataBuilder<K> withKey(K recordKey) {
            this.recordKey = recordKey;
            return this;
        }

        /**
         * In most cases Kafka's partitioner should be used to chose the Kafka partition. Im cases where that
         * is not suitable, it may be specified here.
         *
         * @param partition the partition to use
         * @return this builder
         */
        public OutgoingKafkaRecordMetadataBuilder<K> withPartition(int partition) {
            this.partition = partition;
            return this;
        }

        /**
         * Specify the timestamp for the Kafka record
         *
         * @param timestamp the timestamp
         * @return this builder
         */
        public OutgoingKafkaRecordMetadataBuilder<K> withTimestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Specify headers for Kafka the timestamp for the Kafka record
         *
         * @param headers the headers
         * @return this builder
         */
        public OutgoingKafkaRecordMetadataBuilder<K> withHeaders(Headers headers) {
            this.headers = headers;
            return this;
        }

        /**
         * Specify the headers for the Kafka record
         *
         * @param headers the headers
         * @return this builder
         */
        public OutgoingKafkaRecordMetadataBuilder<K> withHeaders(List<RecordHeader> headers) {
            List<Header> iterable = new ArrayList<>(headers);
            return withHeaders(new RecordHeaders(iterable));
        }

        protected String getTopic() {
            return topic;
        }

        protected K getRecordKey() {
            return recordKey;
        }

        protected int getPartition() {
            return partition;
        }

        protected Instant getTimestamp() {
            return timestamp;
        }

        protected Headers getHeaders() {
            return headers;
        }

        /**
         * Create the {@link OutgoingKafkaRecordMetadata} instance based on the values set in this builder
         *
         * @return a new {@link OutgoingKafkaRecordMetadata} instance
         */
        public OutgoingKafkaRecordMetadata<K> build() {
            return new OutgoingKafkaRecordMetadata<>(topic, recordKey, partition, timestamp, headers);
        }
    }
}
