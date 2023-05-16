package io.smallrye.reactive.messaging.providers.helpers;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;

/**
 * The default implementation of {@link KeyedMulti}
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public class DefaultKeyedMulti<K, V> extends AbstractMulti<V> implements KeyedMulti<K, V> {

    private final Multi<V> grouped;
    private final K key;

    public DefaultKeyedMulti(K key, Multi<V> grouped) {
        this.key = key;
        this.grouped = grouped;
    }

    @Override
    public void subscribe(MultiSubscriber<? super V> subscriber) {
        grouped.subscribe(subscriber);
    }

    @Override
    public K key() {
        return key;
    }

}
