package io.smallrye.reactive.messaging.kafka;

import java.util.stream.Collectors;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import io.vertx.reactivex.kafka.client.consumer.KafkaConsumerRecord;

public class IncomingKafkaRecordMetadata<K, T> implements KafkaMessageMetadata<K> {

    private final K recordKey;
    private final String topic;
    private final int partition;
    private final long timestamp;
    private final TimestampType timestampType;
    private final long offset;
    private final RecordHeaders headers;
    private final KafkaConsumerRecord<K, T> record;

    public IncomingKafkaRecordMetadata(KafkaConsumerRecord<K, T> record) {
        this.record = record;
        this.recordKey = record.key();
        this.topic = record.topic();
        this.partition = record.partition();
        this.timestamp = record.timestamp();
        this.timestampType = record.timestampType();
        this.offset = record.offset();
        if (record.headers() == null) {
            this.headers = new RecordHeaders();
        } else {
            this.headers = new RecordHeaders(record.headers().stream()
                    .map(kh -> new RecordHeader(kh.key(), kh.value().getBytes())).collect(
                            Collectors.toList()));
        }
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
    public long getTimestamp() {
        return timestamp;
    }

    public TimestampType getTimestampType() {
        return timestampType;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public Headers getHeaders() {
        return headers;
    }

    public KafkaConsumerRecord<K, T> getRecord() {
        return record;
    }
}
