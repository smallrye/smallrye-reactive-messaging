package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.mutiny.helpers.Subscriptions.CANCELLED;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.GroupedMulti;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.AbstractMultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.ProcessingOrder;
import io.smallrye.reactive.messaging.kafka.TopicPartitionKey;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.commit.ContextHolder;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * A {@link KafkaCommitHandler} wrapper that applies ordering guarantees for concurrent processing.
 * <p>
 * Messages are grouped (by key or partition) and each group processes messages sequentially,
 * while different groups can process concurrently. All other commit handler operations are
 * delegated to the underlying handler.
 * <p>
 * This strategy supports concurrent processing with ordering guarantees using the
 * `throttled.ordered` configuration. Messages are grouped (by key or partition)
 * and each group processes messages sequentially, while different groups can process
 * concurrently. See {@link ProcessingOrder} for available modes.
 * <p>
 * This decorator works with any commit strategy, but it is particularly useful with the {@code throttled} strategy,
 * as it allows to process messages in order while still benefiting from concurrency.
 *
 * @see ProcessingOrder
 */
public class OrderedStreamHandler extends ContextHolder implements KafkaCommitHandler {

    private final KafkaCommitHandler delegate;
    private final ProcessingOrder processingOrder;
    private final int maxPollRecords;
    private final int maxQueueSizeFactor;
    private final int maxConcurrency;
    private Map<TopicPartitionKey, OrderedGroup> orderedByGroups;

    public OrderedStreamHandler(KafkaConnectorIncomingConfiguration configuration,
            Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            KafkaCommitHandler delegate,
            ProcessingOrder processingOrder) {
        super(vertx, configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                .orElse(60000));
        this.maxPollRecords = getMaxPollRecords(consumer);
        this.maxQueueSizeFactor = configuration.getMaxQueueSizeFactor();
        this.maxConcurrency = configuration.getOrderedMaxConcurrency()
                .or(configuration::getThrottledOrderedMaxConcurrency)
                .orElse(maxPollRecords);
        this.processingOrder = processingOrder;
        this.delegate = delegate;
    }

    private int getMaxPollRecords(KafkaConsumer<?, ?> consumer) {
        String maxPollRecords = (String) consumer.configuration().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        if (maxPollRecords != null) {
            try {
                return Integer.parseInt(maxPollRecords);
            } catch (NumberFormatException ignored) {

            }
        }
        return ConsumerConfig.DEFAULT_MAX_POLL_RECORDS;
    }

    @Override
    public void capture(Context context) {
        super.capture(context);
        if (delegate instanceof ContextHolder) {
            ((ContextHolder) delegate).capture(context);
        }
    }

    @Override
    public void capture(io.vertx.core.Context context) {
        super.capture(context);
        if (delegate instanceof ContextHolder) {
            ((ContextHolder) delegate).capture(context);
        }
    }

    @Override
    public <K, V> Multi<IncomingKafkaRecord<K, V>> decorateStream(Multi<IncomingKafkaRecord<K, V>> consumed) {
        Multi<IncomingKafkaRecord<K, V>> decorated = delegate.decorateStream(consumed);
        return switch (processingOrder) {
            case KEY -> groupOrderBy(decorated, TopicPartitionKey::ofKey);
            case PARTITION -> groupOrderBy(decorated, TopicPartitionKey::ofPartition);
            default -> decorated;
        };
    }

    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        return delegate.received(record);
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        return delegate.handle(record);
    }

    @Override
    public void terminate(boolean graceful) {
        delegate.terminate(graceful);
        if (orderedByGroups != null) {
            orderedByGroups.values().forEach(OrderedGroup::cancel);
            orderedByGroups.clear();
        }

    }

    @Override
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        delegate.partitionsAssigned(partitions);
    }

    @Override
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        if (orderedByGroups != null) {
            orderedByGroups.entrySet().removeIf(entry -> {
                TopicPartitionKey key = entry.getKey();
                if (partitions.contains(key.topicPartition()) && key.key() != null) {
                    entry.getValue().cancel();
                    return true;
                }
                return false;
            });
        }
        delegate.partitionsRevoked(partitions);
    }

    /**
     * Returns the underlying commit handler.
     */
    public KafkaCommitHandler getDelegate() {
        return delegate;
    }

    private <K, V> Multi<IncomingKafkaRecord<K, V>> groupOrderBy(Multi<IncomingKafkaRecord<K, V>> incomingMulti,
            Function<ConsumerRecord<K, V>, TopicPartitionKey> orderBy) {
        long prefetch = (long) maxPollRecords * maxQueueSizeFactor;
        orderedByGroups = new ConcurrentHashMap<>();
        return incomingMulti
                .group()
                .by(message -> {
                    @SuppressWarnings("unchecked")
                    ConsumerRecord<K, V> record = message.getMetadata(IncomingKafkaRecordMetadata.class).get().getRecord();
                    TopicPartitionKey key = orderBy.apply(record);
                    orderedByGroups.computeIfAbsent(key, k -> new OrderedGroup()).incoming();
                    return key;
                }, prefetch)
                .onItem().transformToMulti(g -> new SelfPurgingPauserGroupMulti<>(g, new DemandPauser(), orderedByGroups))
                .merge(maxConcurrency);
    }

    static class OrderedGroup {
        private final AtomicLong pending;
        private volatile Flow.Subscription subscription;

        OrderedGroup() {
            this.pending = new AtomicLong(0);
        }

        public void subscription(Flow.Subscription subscription) {
            this.subscription = subscription;
        }

        public void cancel() {
            if (subscription != null) {
                subscription.cancel();
            }
        }

        public void incoming() {
            pending.incrementAndGet();
        }

        public AtomicLong pending() {
            return pending;
        }

    }

    static class SelfPurgingPauserGroupMulti<K, V>
            extends AbstractMultiOperator<IncomingKafkaRecord<K, V>, IncomingKafkaRecord<K, V>> {

        private final TopicPartitionKey key;
        private final DemandPauser pauser;
        private final OrderedGroup orderedGroup;
        private final Map<TopicPartitionKey, OrderedGroup> orderedByGroups;

        SelfPurgingPauserGroupMulti(GroupedMulti<TopicPartitionKey, IncomingKafkaRecord<K, V>> grouped,
                DemandPauser pauser, Map<TopicPartitionKey, OrderedGroup> orderedByGroups) {
            super(grouped.pauseDemand().using(pauser));
            this.key = grouped.key();
            this.pauser = pauser;
            this.orderedGroup = orderedByGroups.get(this.key);
            this.orderedByGroups = orderedByGroups;
        }

        @Override
        public void subscribe(MultiSubscriber<? super IncomingKafkaRecord<K, V>> subscriber) {
            SelfPurgingPauserGroupMultiProcessor processor = new SelfPurgingPauserGroupMultiProcessor(subscriber);
            upstream.subscribe().withSubscriber(processor);
        }

        class SelfPurgingPauserGroupMultiProcessor
                extends MultiOperatorProcessor<IncomingKafkaRecord<K, V>, IncomingKafkaRecord<K, V>> {
            SelfPurgingPauserGroupMultiProcessor(MultiSubscriber<? super IncomingKafkaRecord<K, V>> downstream) {
                super(downstream);
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                if (compareAndSetUpstreamSubscription(null, subscription) && orderedGroup != null) {
                    // Propagate subscription to downstream.
                    orderedGroup.subscription(this);
                    downstream.onSubscribe(this);
                } else {
                    subscription.cancel();
                }
            }

            @Override
            public void onItem(IncomingKafkaRecord<K, V> item) {
                pauser.pause();
                super.onItem(new OrderedIncomingKafkaRecord<>(item, () -> {
                    if (orderedGroup.pending().decrementAndGet() <= 0L) {
                        orderedGroup.cancel();
                    } else {
                        pauser.resume();
                    }
                }));
            }

            @Override
            public void onCompletion() {
                orderedByGroups.remove(key);
                super.onCompletion();
            }

            @Override
            public void cancel() {
                orderedByGroups.remove(key);
                if (compareAndSwapDownstreamCancellationRequest()) {
                    Flow.Subscription upstream = getAndSetUpstreamSubscription(CANCELLED);
                    if (upstream != null && upstream != CANCELLED) {
                        upstream.cancel(); // cancel the upstream subscription to allow group recreation
                        downstream.onCompletion(); // complete downstream to free the merge slot
                    }
                }
            }
        }
    }
}
