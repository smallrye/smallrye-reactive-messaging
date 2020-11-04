package io.smallrye.reactive.messaging.kafka;

import io.smallrye.mutiny.tuples.Tuple2;

/**
 * Represents a produced Kafka record, so a pair {key, value}.
 * <p>
 * This class is used by application willing to configure the written record.
 * The couple key/value is used for, respectively, the record's key and record's value.
 * Instances are used as message's payload.
 * <p>
 * Both key and value can be {@code null}. Instances of this class are immutable.
 * <p>
 * If the application needs to configure more aspect of the record, use the
 * {@link io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata}.
 * <p>
 * If the attached metadata configures the key, the key provided by the record is overidden.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class Record<K, V> {

    private final Tuple2<K, V> tuple;

    /**
     * Creates a new record.
     *
     * @param key the key, can be {@code null}
     * @param value the value, can be {@code null}
     */
    private Record(K key, V value) {
        tuple = Tuple2.of(key, value);
    }

    /**
     * Creates a new record.
     *
     * @param key the key, can be {@code null}
     * @param value the value, can be {@code null}
     * @param <K> the type of the key
     * @param <V> the type of the value
     */
    public static <K, V> Record<K, V> of(K key, V value) {
        return new Record<>(key, value);
    }

    /**
     * @return the key, may be {@code null}
     */
    public K key() {
        return tuple.getItem1();
    }

    /**
     * @return the value, may be {@code null}
     */
    public V value() {
        return tuple.getItem2();
    }

    /**
     * Creates a new instance of {@link Record} with given key and the value from the current record.
     *
     * @param key the new key, can be {@code null}
     * @param <T> the type of the new key
     * @return the new record
     */
    public <T> Record<T, V> withKey(T key) {
        return new Record<>(key, value());
    }

    /**
     * Creates a new instance of {@link Record} with the key from the current record and the new value.
     *
     * @param value the new value, can be {@code null}
     * @param <T> the type of the new value
     * @return the new record
     */
    public <T> Record<K, T> withValue(T value) {
        return new Record<>(key(), value);
    }
}
