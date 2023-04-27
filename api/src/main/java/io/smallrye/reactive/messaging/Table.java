package io.smallrye.reactive.messaging;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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

    Table<K, V> withEmitOnChange();

    <T> Table<K, T> map(BiFunction<K, V, T> mapper);

    <T> Table<T, V> mapKey(BiFunction<K, V, T> mapper);

    <T> Table<K, T> chain(BiFunction<K, V, Uni<T>> mapper);

    <T> Table<T, V> chainKey(BiFunction<K, V, Uni<T>> mapper);

    Table<K, V> filter(BiPredicate<K, V> predicate);

    Table<K, V> filterKey(Predicate<K> predicate);

    Table<K, V> filterKey(K keyToMatch);

}
