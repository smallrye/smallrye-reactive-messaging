package io.smallrye.reactive.messaging.kafka;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class OutgoingKafkaRecordMetadata<K> implements KafkaMessageMetadata<K> {

    private final String topic;
    private final K recordKey;
    private final int partition;
    private final long timestamp;
    private final Headers headers;

    public static <K> OutgoingKafkaRecordMetadataBuilder<K> builder() {
        return new OutgoingKafkaRecordMetadataBuilder<>();
    }

    public OutgoingKafkaRecordMetadata(String topic, K key, int partition, long timestamp,
            Headers headers) {
        this.topic = topic;
        this.recordKey = key;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public K getKey() {
        return recordKey;
    }

    public static final class OutgoingKafkaRecordMetadataBuilder<K> {
        private String topic;
        private K recordKey;
        private int partition = -1;
        private long timestamp = -1;
        private Headers headers;

        public OutgoingKafkaRecordMetadataBuilder<K> withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public OutgoingKafkaRecordMetadataBuilder<K> withKey(K recordKey) {
            this.recordKey = recordKey;
            return this;
        }

        public OutgoingKafkaRecordMetadataBuilder<K> withPartition(int partition) {
            this.partition = partition;
            return this;
        }

        public OutgoingKafkaRecordMetadataBuilder<K> withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public OutgoingKafkaRecordMetadataBuilder<K> withHeaders(Headers headers) {
            this.headers = headers;
            return this;
        }

        public OutgoingKafkaRecordMetadata<K> build() {
            return new OutgoingKafkaRecordMetadata<>(topic, recordKey, partition, timestamp, headers);
        }

        public OutgoingKafkaRecordMetadataBuilder<K> withHeaders(List<RecordHeader> headers) {
            List<Header> iterable = new ArrayList<>(headers);
            return withHeaders(new RecordHeaders(iterable));
        }
    }
}
