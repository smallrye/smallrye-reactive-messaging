package io.smallrye.reactive.messaging.kafka;

import java.time.Instant;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * @param <K> the type of the key
 * @param <T> the type of the value
 * @deprecated Use {@link KafkaRecord instead}
 */
@Deprecated
public interface KafkaMessage<K, T> extends Message<T>, KafkaRecord<K, T> {

    /**
     * Creates a new outgoing kafka message.
     *
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing kafka message
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(K key, T value) {
        return new OutgoingKafkaRecord<>(null, key, value, null, -1,
                new RecordHeaders(), null, null, null);
    }

    /**
     * Creates a new outgoing kafka message.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing kafka message
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(String topic, K key, T value) {
        return new OutgoingKafkaRecord<>(topic, key, value, null, -1, new RecordHeaders(), null, null, null);
    }

    /**
     * Creates a new outgoing kafka message.
     *
     * @param topic the topic, must not be {@code null}
     * @param key the key, can be {@code null}
     * @param value the value / payload, must not be {@code null}
     * @param timestamp the timestamp, can be null to indicate no timestamp
     * @param partition the partition, can be {@code -1} to indicate no partition
     * @param <K> the type of the key
     * @param <T> the type of the value
     * @return the new outgoing kafka message
     */
    static <K, T> OutgoingKafkaRecord<K, T> of(String topic, K key, T value, Instant timestamp, int partition) {
        return new OutgoingKafkaRecord<>(topic, key, value, timestamp, partition, new RecordHeaders(), null, null, null);
    }

}
