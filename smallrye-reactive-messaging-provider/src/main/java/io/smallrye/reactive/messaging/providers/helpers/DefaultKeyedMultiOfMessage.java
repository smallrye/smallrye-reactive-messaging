package io.smallrye.reactive.messaging.providers.helpers;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.keyed.KeyedMulti;

/**
 * The default implementation of {@link KeyedMulti} for the {@code KeyedMulti<K, Message<V>>}.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public class DefaultKeyedMultiOfMessage<K, V> extends AbstractMulti<Message<V>> implements KeyedMulti<K, Message<V>> {

    private final Multi<? extends Message<V>> grouped;
    private final K key;

    public DefaultKeyedMultiOfMessage(K key, Multi<? extends Message<V>> grouped) {
        this.key = key;
        this.grouped = grouped;
    }

    @Override
    public void subscribe(MultiSubscriber<? super Message<V>> subscriber) {
        grouped.subscribe(subscriber);
    }

    @Override
    public K key() {
        return key;
    }

}
