package io.smallrye.reactive.messaging.providers.extension;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
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
    private final boolean emitOnChange;

    public DefaultTable(Multi<? extends Message<V>> upstream, Function<Message<V>, Tuple2<K, V>> tupleExtractor) {
        this(Collections.emptyMap(), upstream, tupleExtractor, false);
    }

    public DefaultTable(Multi<? extends Message<V>> upstream, Function<Message<V>, Tuple2<K, V>> tupleExtractor,
            boolean emitOnChange) {
        this(Collections.emptyMap(), upstream, tupleExtractor, emitOnChange);
    }

    public DefaultTable(Map<K, V> data, Multi<? extends Message<V>> upstream,
            Function<Message<V>, Tuple2<K, V>> tupleExtractor, boolean emitOnChange) {
        this(data,
                ParameterValidation.nonNull(upstream, "upstream")
                        .onItem().transformToUniAndConcatenate(m -> Uni.createFrom().completionStage(m.ack()).map(x -> m))
                        .onItem().transform(tupleExtractor),
                emitOnChange,
                true);
    }

    public DefaultTable(Map<K, V> data, Multi<? extends Tuple2<K, V>> upstream, boolean emitOnChange,
            boolean subscribeOnCreation) {
        this.data = new ConcurrentHashMap<>(data);
        this.upstream = ParameterValidation.nonNull(upstream, "upstream")
                .withContext((multi, context) -> {
                    context.put("table_data", this.data);
                    return multi;
                })
                .attachContext()
                .map(t -> {
                    Tuple2<K, V> tuple = t.get();
                    Map<K, V> tableData = t.context().get("table_data");
                    K key = tuple.getItem1();
                    V value = tuple.getItem2();
                    V previous = value == null ? tableData.remove(key) : tableData.put(key, value);
                    if (emitOnChange && Objects.equals(previous, value)) {
                        // TODO Check if we can do this generalization
                        return Tuple2.<K, V> of(null, null);
                    }
                    return tuple;
                }).filter(t -> t.getItem1() != null || t.getItem2() != null)
                .broadcast().toAllSubscribers();
        this.emitOnChange = emitOnChange;
        if (subscribeOnCreation) {
            this.subscribe().with(t -> {
            });
        }
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

    public <T> Table<K, T> map(BiFunction<K, V, T> mapper) {
        return new DefaultTable<>(Collections.emptyMap(),
                this.upstream.map(t -> Tuple2.of(t.getItem1(), mapper.apply(t.getItem1(), t.getItem2()))),
                emitOnChange, false);
    }

    public <T> Table<T, V> mapKey(BiFunction<K, V, T> mapper) {
        return new DefaultTable<>(Collections.emptyMap(),
                this.upstream.map(t -> Tuple2.of(mapper.apply(t.getItem1(), t.getItem2()), t.getItem2())),
                emitOnChange, false);
    }

    public <T> Table<K, T> chain(BiFunction<K, V, Uni<T>> mapper) {
        return new DefaultTable<>(Collections.emptyMap(), this.upstream
                .onItem().transformToUniAndConcatenate(t -> mapper.apply(t.getItem1(), t.getItem2())
                        .map(v -> Tuple2.of(t.getItem1(), v))),
                emitOnChange, false);
    }

    public <T> Table<T, V> chainKey(BiFunction<K, V, Uni<T>> mapper) {
        return new DefaultTable<>(Collections.emptyMap(), this.upstream
                .onItem().transformToUniAndConcatenate(t -> mapper.apply(t.getItem1(), t.getItem2())
                        .map(k -> Tuple2.of(k, t.getItem2()))),
                emitOnChange, false);
    }

    public Table<K, V> filter(BiPredicate<K, V> predicate) {
        return new DefaultTable<>(Collections.emptyMap(), this.upstream
                .filter(t -> predicate.test(t.getItem1(), t.getItem2())),
                emitOnChange, false);
    }

    public Table<K, V> filterKey(Predicate<K> predicate) {
        return new DefaultTable<>(Collections.emptyMap(), this.upstream
                .filter(t -> predicate.test(t.getItem1())),
                emitOnChange, false);
    }

    public Table<K, V> withEmitOnChange() {
        return new DefaultTable<>(Collections.emptyMap(), this.upstream, true, false);
    }

    public Table<K, V> filterKey(K keyToMatch) {
        return filterKey(k -> Objects.equals(k, keyToMatch));
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
