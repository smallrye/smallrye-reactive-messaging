package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

public class IncomingKafkaRecordMetadata<K, T> implements KafkaMessageMetadata<K> {

    private final K recordKey;
    private final String topic;
    private final int partition;
    private final TimestampType timestampType;
    private final long offset;
    private final ConsumerRecord<K, T> record;

    public IncomingKafkaRecordMetadata(ConsumerRecord<K, T> record) {
        this.record = record;
        this.recordKey = record.key();
        this.topic = record.topic();
        this.partition = record.partition();
        this.timestampType = record.timestampType();
        this.offset = record.offset();
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public K getKey() {
        return recordKey;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public Instant getTimestamp() {
        return Instant.ofEpochMilli(record.timestamp());
    }

    public TimestampType getTimestampType() {
        return timestampType;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public Headers getHeaders() {
        Headers headers = new RecordHeaders();
        if (record.headers() != null) {
            for (Header header : record.headers()) {
                headers.add(new RecordHeader(header.key(), header.value()));
            }
        }
        return headers;
    }

    public ConsumerRecord<K, T> getRecord() {
        return record;
    }
}
