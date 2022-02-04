package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public interface KafkaRecord<K, T> extends Message<T>, ContextAwareMessage<T> {

    static <K, T> OutgoingKafkaRecord<K, T> from(Message<T> message) {
        return OutgoingKafkaRecord.from(message);
    }

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
        return new OutgoingKafkaRecord<>(null, key, value, null, -1,
                new RecordHeaders(), null, null, null);
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
        return new OutgoingKafkaRecord<>(topic, key, value, null, -1, new RecordHeaders(), null, null, null);
    }

    /**
     * Creates a new outgoing Kafka record.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param timestamp the timestamp, can be null to indicate no timestamp
     * @param partition the partition, can be {@code -1} to indicate no partition
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing Kafka record
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(String topic, K key, T value, Instant timestamp, int partition) {
        return new OutgoingKafkaRecord<>(topic, key, value, timestamp, partition, new RecordHeaders(), null, null, null);
    }

    K getKey();

    String getTopic();

    int getPartition();

    Instant getTimestamp();

    Headers getHeaders();

}
