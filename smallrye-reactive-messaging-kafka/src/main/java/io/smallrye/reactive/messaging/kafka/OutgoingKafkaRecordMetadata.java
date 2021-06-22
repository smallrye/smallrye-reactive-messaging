package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;
import java.util.List;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * @deprecated use {@link io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata} instead
 */
@Deprecated
public class OutgoingKafkaRecordMetadata<K> extends io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata<K>
        implements KafkaMessageMetadata<K> {

    public static <K> OutgoingKafkaRecordMetadataBuilder<K> builder() {
        return new OutgoingKafkaRecordMetadataBuilder<>();
    }

    public OutgoingKafkaRecordMetadata(String topic, K key, int partition, Instant timestamp,
            Headers headers) {
        super(topic, key, partition, timestamp, headers);
    }

    public static final class OutgoingKafkaRecordMetadataBuilder<K>
            extends io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata.OutgoingKafkaRecordMetadataBuilder<K> {

        @Override
        public OutgoingKafkaRecordMetadataBuilder<K> withTopic(String topic) {
            super.withTopic(topic);
            return this;
        }

        @Override
        public OutgoingKafkaRecordMetadataBuilder<K> withKey(K recordKey) {
            super.withKey(recordKey);
            return this;
        }

        @Override
        public OutgoingKafkaRecordMetadataBuilder<K> withPartition(int partition) {
            super.withPartition(partition);
            return this;
        }

        @Override
        public OutgoingKafkaRecordMetadataBuilder<K> withTimestamp(Instant timestamp) {
            super.withTimestamp(timestamp);
            return this;
        }

        @Override
        public OutgoingKafkaRecordMetadataBuilder<K> withHeaders(Headers headers) {
            super.withHeaders(headers);
            return this;
        }

        @Override
        public OutgoingKafkaRecordMetadataBuilder<K> withHeaders(List<RecordHeader> headers) {
            super.withHeaders(headers);
            return this;
        }

        public OutgoingKafkaRecordMetadata<K> build() {
            return new OutgoingKafkaRecordMetadata<>(
                    getTopic(), getRecordKey(), getPartition(), getTimestamp(), getHeaders());
        }
    }
}
