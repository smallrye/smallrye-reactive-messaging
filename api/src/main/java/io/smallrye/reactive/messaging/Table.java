package io.smallrye.reactive.messaging;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;

/**
 * Represents a table, i.e. a data structure storing the last value associated to a given key.
 *
 * @param <K> the class of the key
 * @param <V> the class of the value
 */
public interface Table<K, V> extends Multi<Tuple2<K, V>> {

    Set<K> keys();

    Collection<V> values();

    Map<K, V> fetch(Collection<K> keys);

    Map<K, V> toMap();

    Multi<Map<K, V>> toMapStream();

    V get(K key);

}
