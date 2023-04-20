package io.smallrye.reactive.messaging.providers.extension;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.ParameterValidation;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.Table;

public class DefaultTable<K, V> extends AbstractMulti<Tuple2<K, V>> implements Table<K, V> {

    /**
     * The upstream {@link Multi}.
     */
    protected final Multi<? extends Tuple2<K, V>> upstream;

    // High memory consumption
    private final Map<K, V> data;

    public DefaultTable(Multi<? extends Message<V>> upstream, Function<Message<V>, Tuple2<K, V>> tupleExtractor) {
        this(Collections.emptyMap(), upstream, tupleExtractor);
    }

    public DefaultTable(Map<K, V> data, Multi<? extends Message<V>> upstream,
            Function<Message<V>, Tuple2<K, V>> tupleExtractor) {
        this.data = new ConcurrentHashMap<>(data);
        this.upstream = ParameterValidation.nonNull(upstream, "upstream")
                .onItem().transformToUniAndConcatenate(m -> {
                    Tuple2<K, V> tuple = tupleExtractor.apply(m);
                    return Uni.createFrom().completionStage(m.ack())
                            .map(x -> {
                                if (tuple.getItem2() == null) {
                                    this.data.remove(tuple.getItem1());
                                } else {
                                    this.data.put(tuple.getItem1(), tuple.getItem2());
                                }
                                return tuple;
                            });
                })
                .broadcast().toAllSubscribers();
        this.subscribe().with(t -> {
        });
    }

    @Override
    public void subscribe(MultiSubscriber<? super Tuple2<K, V>> downstream) {
        if (downstream == null) {
            throw new NullPointerException("Subscriber is `null`");
        }
        this.upstream.subscribe().withSubscriber(downstream);
    }

    @Override
    public Set<K> keys() {
        return this.data.keySet();
    }

    @Override
    public Collection<V> values() {
        return this.data.values();
    }

    @Override
    public Map<K, V> fetch(Collection<K> keys) {
        return this.data.entrySet()
                .stream().filter(e -> keys.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Map<K, V> toMap() {
        return new HashMap<>(this.data);
    }

    @Override
    public Multi<Map<K, V>> toMapStream() {
        return this.onItem().scan(() -> new ConcurrentHashMap<>(this.data), (m, t) -> {
            m.put(t.getItem1(), t.getItem2());
            return m;
        });
    }

    @Override
    public V get(K key) {
        return this.data.get(key);
    }

}
