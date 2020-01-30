package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

public interface KafkaRecord<K, T> extends Message<T> {

    /**
     * Creates a new outgoing Kafka record.
     *
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing Kafka record
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(K key, T value) {
        return new OutgoingKafkaRecord<>(null, key, value, -1, -1,
                new RecordHeaders(), null);
    }

    /**
     * Creates a new outgoing Kafka record.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing Kafka record
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(String topic, K key, T value) {
        return new OutgoingKafkaRecord<>(topic, key, value, -1, -1, new RecordHeaders(), null);
    }

    /**
     * Creates a new outgoing Kafka record.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param timestamp the timestamp, can be {@code -1} to indicate no timestamp
     * @param partition the partition, can be {@code -1} to indicate no partition
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing Kafka record
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(String topic, K key, T value, long timestamp, int partition) {
        return new OutgoingKafkaRecord<>(topic, key, value, timestamp, partition, new RecordHeaders(), null);
    }

    K getKey();

    String getTopic();

    int getPartition();

    long getTimestamp();

    Headers getHeaders();

}
