package io.smallrye.reactive.messaging.keyed;

import io.smallrye.mutiny.GroupedMulti;

/**
 * A keyed stream.
 *
 * @param <K> the type of the key
 * @param <V> the type of the values
 */
public interface KeyedMulti<K, V> extends GroupedMulti<K, V> {

}
