package io.smallrye.reactive.messaging.kafka.api;

import java.time.Instant;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

/**
 * Contains information about messages received from a channel backed by Kafka.
 * Generally this will be created by the framework, and users should not construct instances of this class.
 *
 * @param <K> The Kafka record key type
 * @param <T> The payload type
 */
public class IncomingKafkaRecordMetadata<K, T> implements KafkaMessageMetadata<K> {

    private final ConsumerRecord<K, T> record;
    private volatile Headers headers;
    private final String channel;
    private final int index;

    /**
     * Constructor
     *
     * @param record the underlying record received from Kafka
     */
    public IncomingKafkaRecordMetadata(ConsumerRecord<K, T> record, String channel, int index) {
        this.record = record;
        this.channel = channel;
        this.index = index;
    }

    public IncomingKafkaRecordMetadata(ConsumerRecord<K, T> record, String channel) {
        this(record, channel, -1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTopic() {
        return record.topic();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getKey() {
        return record.key();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPartition() {
        return record.partition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Instant getTimestamp() {
        return Instant.ofEpochMilli(record.timestamp());
    }

    /**
     * Get the timestamp type
     *
     * @return the timestamp type
     */
    public TimestampType getTimestampType() {
        return record.timestampType();
    }

    /**
     * Get the offset
     *
     * @return the offset
     */
    public long getOffset() {
        return record.offset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Headers getHeaders() {
        if (headers == null) {
            synchronized (this) {
                if (headers == null) {
                    Headers headers = new RecordHeaders();
                    if (record.headers() != null) {
                        for (Header header : record.headers()) {
                            headers.add(new RecordHeader(header.key(), header.value()));
                        }
                    }
                    this.headers = headers;
                }
            }
        }
        return headers;
    }

    /**
     * Get the underlying Kafka ConsumerRecord
     *
     * @return the underlying Kafka ConsumerRecord
     */
    public ConsumerRecord<K, T> getRecord() {
        return record;
    }

    public String getChannel() {
        return channel;
    }

    public int getConsumerIndex() {
        return index;
    }
}
