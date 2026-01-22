package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.mutiny.helpers.Subscriptions.CANCELLED;
import static io.smallrye.reactive.messaging.kafka.ProcessingOrder.UNORDERED;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;
import static io.smallrye.reactive.messaging.kafka.impl.TopicPartitions.getTopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.GroupedMulti;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.AbstractMultiOperator;
import io.smallrye.mutiny.operators.multi.MultiOperatorProcessor;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.ProcessingOrder;
import io.smallrye.reactive.messaging.kafka.TopicPartitionKey;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.OrderedIncomingKafkaRecord;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.mutiny.core.Vertx;

/**
 * Will keep track of received messages and commit to the next offset after the latest
 * ACKed message in sequence. Will commit periodically as defined by `auto.commit.interval.ms` (default: 5000)
 * <p>
 * This strategy mimics the behavior of the kafka consumer when `enable.auto.commit`
 * is `true`.
 * <p>
 * This strategy supports concurrent processing with ordering guarantees using the
 * `throttled.ordered` configuration. Messages are grouped (by key or partition)
 * and each group processes messages sequentially, while different groups can process
 * concurrently. See {@link ProcessingOrder} for available modes.
 * <p>
 * The connector will be marked as unhealthy in the presence of any received record that has gone
 * too long without being processed as defined by `throttled.unprocessed-record-max-age.ms` (default: 60000).
 * If `throttled.unprocessed-record-max-age.ms` is set to less than or equal to 0 then will not
 * perform any health check (this might lead to running out of memory).
 * <p>
 * This strategy guarantees at-least-once delivery even if the channel performs
 * asynchronous processing.
 * <p>
 * To use set `commit-strategy` to `throttled`.
 */
public class KafkaThrottledLatestProcessedCommit extends ContextHolder implements KafkaCommitHandler {

    private final Map<TopicPartition, OffsetStore> offsetStores = new HashMap<>();

    private final String groupId;
    private final KafkaConsumer<?, ?> consumer;
    private final BiConsumer<Throwable, Boolean> reportFailure;
    private final int unprocessedRecordMaxAge;
    private final int autoCommitInterval;
    private final int maxQueueSizeFactor;
    private final ProcessingOrder processingOrder;
    private final int maxPollRecords;
    private final int maxConcurrency;
    private volatile long timerId = -1;
    private final Collection<TopicPartition> assignments = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Map<TopicPartitionKey, OrderedGroup> orderedByGroups;

    @ApplicationScoped
    @Identifier(Strategy.THROTTLED)
    public static class Factory implements KafkaCommitHandler.Factory {

        @Override
        public KafkaThrottledLatestProcessedCommit create(
                KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            String groupId = (String) consumer.configuration().get(ConsumerConfig.GROUP_ID_CONFIG);
            int unprocessedRecordMaxAge = config.getThrottledUnprocessedRecordMaxAgeMs();
            ProcessingOrder processingOrder = config.getThrottledOrdered().map(ProcessingOrder::of).orElse(UNORDERED);
            int autoCommitInterval = config.config()
                    .getOptionalValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.class)
                    .orElse(5000);
            int defaultTimeout = config.config()
                    .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                    .orElse(60000);
            int maxQueueSizeFactor = config.getMaxQueueSizeFactor();
            int maxPollRecords = getMaxPollRecords(consumer);
            log.settingCommitInterval(groupId, autoCommitInterval);
            if (unprocessedRecordMaxAge <= 0) {
                log.disableThrottledCommitStrategyHealthCheck(groupId);
            } else {
                log.setThrottledCommitStrategyReceivedRecordMaxAge(groupId, unprocessedRecordMaxAge);
            }
            return new KafkaThrottledLatestProcessedCommit(groupId, vertx, consumer, reportFailure, unprocessedRecordMaxAge,
                    autoCommitInterval, defaultTimeout, maxQueueSizeFactor, processingOrder, maxPollRecords,
                    config.getThrottledOrderedMaxConcurrency().orElse(maxPollRecords));
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
    }

    private KafkaThrottledLatestProcessedCommit(
            String groupId,
            Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            BiConsumer<Throwable, Boolean> reportFailure,
            int unprocessedRecordMaxAge,
            int autoCommitInterval,
            int defaultTimeout,
            int maxQueueSizeFactor,
            ProcessingOrder processingOrder,
            int maxPollRecords,
            int maxConcurrency) {
        super(vertx, defaultTimeout);
        this.groupId = groupId;
        this.consumer = consumer;
        this.reportFailure = reportFailure;
        this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        this.autoCommitInterval = autoCommitInterval;
        this.maxQueueSizeFactor = maxQueueSizeFactor;
        this.processingOrder = processingOrder;
        this.maxPollRecords = maxPollRecords;
        this.maxConcurrency = maxConcurrency;
    }

    /**
     * New partitions are assigned.
     * This method is called from the Kafka poll thread.
     *
     * @param partitions the list of partitions that are now assigned to the consumer
     *        (may include partitions previously assigned to the consumer)
     */
    @Override
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        runOnContextAndAwait(() -> {
            stopFlushAndCheckHealthTimer();
            assignments.addAll(partitions);
            if (!partitions.isEmpty() || !offsetStores.isEmpty()) {
                startFlushAndCheckHealthTimer();
            }
            return null;
        });
    }

    /**
     * Revoked partitions.
     * This method is called from the Kafka pool thread.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need to be revoked
     *        (may not include all currently assigned partitions).
     */
    @Override
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        Tuple2<Map<TopicPartition, OffsetAndMetadata>, Boolean> result = runOnContextAndAwait(() -> {
            stopFlushAndCheckHealthTimer();
            assignments.removeAll(partitions);
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

            // Remove all handled partitions that are not in the given list of partitions
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            for (TopicPartition partition : new HashSet<>(offsetStores.keySet())) {
                if (!assignments.contains(partition)) { // revoked partition - remove and compute last commit
                    OffsetStore store = offsetStores.remove(partition);
                    if (store != null) {

                        long largestOffset = store.clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset();
                        if (largestOffset > -1) {
                            toCommit.put(partition, new OffsetAndMetadata(largestOffset + 1L, null));
                            log.partitionRevokedCollectingRecordsToCommit(partition, largestOffset + 1);
                        }
                    }
                }
            }
            return Tuple2.of(toCommit, !offsetStores.isEmpty());
        });

        if (!result.getItem1().isEmpty()) {
            // We are on the polling thread, we can use synchronous (blocking) commit
            consumer.unwrap().commitSync(result.getItem1());
        }

        if (result.getItem2()) {
            runOnContext(this::startFlushAndCheckHealthTimer);
        }
    }

    /**
     * Cancel the existing timer.
     * Must be called from the event loop.
     */
    private void stopFlushAndCheckHealthTimer() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
            timerId = -1;
        }
    }

    /**
     * Schedule the next commit.
     * Must be called form the event loop.
     */
    private void startFlushAndCheckHealthTimer() {
        timerId = vertx.setTimer(autoCommitInterval, x -> runOnContext(() -> this.flushAndCheckHealth(x)));
    }

    @Override
    public <K, V> Multi<IncomingKafkaRecord<K, V>> decorateStream(Multi<IncomingKafkaRecord<K, V>> consumed) {
        // preserve order by key if configured
        return switch (processingOrder) {
            case KEY -> groupOrderBy(consumed, TopicPartitionKey::ofKey);
            case PARTITION -> groupOrderBy(consumed, TopicPartitionKey::ofPartition);
            default -> consumed;
        };
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

    /**
     * Received a new record from Kafka.
     * This method is called from a Vert.x event loop.
     *
     * @param record the record
     * @param <K> the key
     * @param <V> the value
     * @return the record emitted once everything has been done
     */
    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        TopicPartition recordsTopicPartition = getTopicPartition(record);

        OffsetStore offsetStore = offsetStores.get(recordsTopicPartition);
        Uni<OffsetStore> uni;
        if (offsetStore == null) {
            uni = consumer.committed(recordsTopicPartition)
                    .emitOn(this::runOnContext) // Switch back to event loop
                    .onItem().transform(offsets -> {
                        OffsetAndMetadata lastCommitted = offsets.get(recordsTopicPartition);
                        OffsetStore store = new OffsetStore(recordsTopicPartition, unprocessedRecordMaxAge,
                                lastCommitted == null ? -1 : lastCommitted.offset() - 1);
                        offsetStores.put(recordsTopicPartition, store);
                        return store;
                    });
        } else {
            uni = Uni.createFrom().item(offsetStore);
        }

        return uni
                .onItem().invoke(store -> {
                    store.received(record.getOffset());
                    if (timerId < 0) {
                        startFlushAndCheckHealthTimer();
                    }
                })
                .onItem().transform(x -> record);
    }

    /**
     * Must be called from the event loop.
     *
     * @return the map of partition -> offset that can be committed.
     */
    private Map<TopicPartition, Long> clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping() {
        cleanupPartitionOffsetStore();

        Map<TopicPartition, Long> offsetsMapping = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetStore> entry : new HashSet<>(offsetStores.entrySet())) {
            if (assignments.contains(entry.getKey())) {
                long offset = entry.getValue()
                        .clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset();
                if (offset > -1) {
                    offsetsMapping.put(entry.getKey(), offset);
                }
            }
        }

        return offsetsMapping;
    }

    /**
     * A message has been acknowledged.
     * This method is NOT necessarily called on an event loop.
     *
     * @param record the record
     * @param <K> the key
     * @param <V> the value
     * @return a completion stage indicating when the commit complete
     */
    @Override
    public <K, V> Uni<Void> handle(final IncomingKafkaRecord<K, V> record) {
        return Uni.createFrom().completionStage(VertxContext.runOnEventLoopContext(context.getDelegate(), f -> {
            TopicPartition topicPartition = getTopicPartition(record);
            OffsetStore store = offsetStores
                    .get(topicPartition);

            /*
             * If there is no store for the record that means the topic partitions was revoked
             * for this instance but the record was ACKed after the fact. In this case not much to
             * do but ignore the message. There likely will be a duplicate consumption.
             */
            if (store != null) {
                store.processed(record.getOffset());
            } else {
                log.acknowledgementFromRevokedTopicPartition(
                        record.getOffset(), topicPartition, groupId, assignments);
            }
            record.runOnMessageContext(() -> f.complete(null));
        }));
    }

    /**
     * Always called from the event loop.
     *
     * @param ignored the timer id.
     */
    @SuppressWarnings("unused")
    private void flushAndCheckHealth(long ignored) {
        Map<TopicPartition, Long> offsetsMapping = clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping();
        if (!offsetsMapping.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(offsetsMapping);
            consumer.commit(offsets)
                    .subscribe().with(
                            a -> {
                                log.committed(offsets);
                                this.startFlushAndCheckHealthTimer();
                            },
                            f -> {
                                log.failedToCommit(offsets, f);
                                this.startFlushAndCheckHealthTimer();
                            });
        } else {
            this.startFlushAndCheckHealthTimer();
        }

        if (this.unprocessedRecordMaxAge > 0) {
            for (OffsetStore store : offsetStores.values()) {
                long millis = store.hasTooManyMessagesWithoutAck();
                if (millis != -1) {
                    OffsetReceivedAt received = store.receivedOffsets.peek();
                    if (received != null) {
                        long lastOffset = store.getLastProcessedOffset();
                        TooManyMessagesWithoutAckException exception = new TooManyMessagesWithoutAckException(
                                store.topicPartition,
                                received.offset,
                                millis / 1000,
                                store.receivedOffsets.size(),
                                lastOffset);
                        this.reportFailure.accept(exception, true);
                    }
                }
            }
        }

    }

    private static class OffsetReceivedAt {
        private final long offset;
        private final long receivedAt;

        private OffsetReceivedAt(long offset, long receivedAt) {
            this.offset = offset;
            this.receivedAt = receivedAt;
        }

        static OffsetReceivedAt received(long offset) {
            return new OffsetReceivedAt(offset, System.currentTimeMillis());
        }

        public long getOffset() {
            return offset;
        }

        public long getReceivedAt() {
            return receivedAt;
        }
    }

    private class OffsetStore {

        private final TopicPartition topicPartition;
        private final Queue<OffsetReceivedAt> receivedOffsets = new PriorityQueue<>(
                Comparator.comparingLong(OffsetReceivedAt::getOffset));
        private final Set<Long> processedOffsets = new HashSet<>();
        private final int unprocessedRecordMaxAge;
        private final AtomicLong unProcessedTotal = new AtomicLong();
        private long lastProcessedOffset;

        OffsetStore(TopicPartition topicPartition, int unprocessedRecordMaxAge, long lastProcessedOffset) {
            this.topicPartition = topicPartition;
            this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
            log.initializeStoreAtPosition(topicPartition, lastProcessedOffset);
            this.lastProcessedOffset = lastProcessedOffset;
        }

        long getLastProcessedOffset() {
            return lastProcessedOffset;
        }

        void received(long offset) {
            if (offset > lastProcessedOffset) {
                this.receivedOffsets.offer(OffsetReceivedAt.received(offset));
                unProcessedTotal.incrementAndGet();
            } else {
                log.receivedOutdatedOffset(topicPartition, offset, lastProcessedOffset);
            }
        }

        void processed(long offset) {
            processedOffsets.add(offset);
        }

        long clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset() {
            if (!processedOffsets.isEmpty()) {
                long largestSequentialProcessedOffset = -1;

                while (!receivedOffsets.isEmpty()) {
                    if (!processedOffsets.remove(receivedOffsets.peek().getOffset())) {
                        break;
                    }
                    unProcessedTotal.decrementAndGet();
                    OffsetReceivedAt poll = receivedOffsets.poll();
                    if (poll != null) {
                        largestSequentialProcessedOffset = poll.getOffset();
                    }
                }

                if (largestSequentialProcessedOffset > -1) {
                    lastProcessedOffset = largestSequentialProcessedOffset;
                    removeReceivedOffsetsFromLastProcessedOffset();
                    return largestSequentialProcessedOffset;
                }
            }

            removeReceivedOffsetsFromLastProcessedOffset();

            return -1;
        }

        private void removeReceivedOffsetsFromLastProcessedOffset() {
            // Remove received offset from previous assignments if any
            receivedOffsets.removeIf(o -> o.getOffset() <= lastProcessedOffset);
        }

        long hasTooManyMessagesWithoutAck() {
            if (receivedOffsets.isEmpty() || !isStillAssigned()) {
                return -1;
            }
            OffsetReceivedAt peek = receivedOffsets.peek();
            if (peek == null) {
                return -1;
            }
            long elapsed = System.currentTimeMillis() - peek.getReceivedAt();
            long lag = receivedOffsets.size();
            boolean waitedTooLong = elapsed > unprocessedRecordMaxAge;
            if (waitedTooLong) {
                log.waitingForAckForTooLong(peek.getOffset(), topicPartition, elapsed / 1000, unprocessedRecordMaxAge,
                        lag, lastProcessedOffset);
                return elapsed;
            }
            return -1;
        }

        private boolean isStillAssigned() {
            // If the topic/partition is not assigned to us already, the store will be cleared eventually.
            return assignments.contains(topicPartition);
        }

        long getUnprocessedCount() {
            return unProcessedTotal.get();
        }
    }

    private void cleanupPartitionOffsetStore() {
        for (TopicPartition partition : new HashSet<>(offsetStores.keySet())) {
            if (!assignments.contains(partition)) {
                log.removingPartitionFromStore(partition, assignments);
                offsetStores.remove(partition);
            }
        }
    }

    public static class TooManyMessagesWithoutAckException extends NoStackTraceThrowable {
        public TooManyMessagesWithoutAckException(TopicPartition topic, long offset, long time, long queueSize,
                long lastCommittedOffset) {
            super(String.format("The record %d from topic/partition '%s' has waited for %d seconds to be acknowledged. " +
                    "At the moment %d messages from this partition are awaiting acknowledgement. The last committed " +
                    "offset for this partition was %d.", offset, topic, time, queueSize, lastCommittedOffset));
        }
    }

    @Override
    public void terminate(boolean graceful) {
        if (graceful) {
            long stillUnprocessed = waitForProcessing();
            if (stillUnprocessed > 0) {
                log.messageStillUnprocessedAfterTimeout(stillUnprocessed);
            }
        }

        commitAllAndAwait();
        runOnContextAndAwait(() -> {
            if (orderedByGroups != null) {
                orderedByGroups.values().forEach(OrderedGroup::cancel);
                orderedByGroups.clear();
            }
            offsetStores.clear();
            stopFlushAndCheckHealthTimer();
            return null;
        });
    }

    private long waitForProcessing() {
        int attempt = autoCommitInterval / 100;
        for (int i = 0; i < attempt; i++) {
            long sum = offsetStores.values().stream().map(OffsetStore::getUnprocessedCount).mapToLong(l -> l).sum();
            if (sum == 0) {
                return sum;
            }
            log.waitingForMessageProcessing(sum);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        return offsetStores.values().stream().map(OffsetStore::getUnprocessedCount).mapToLong(l -> l).sum();

    }

    private void commitAllAndAwait() {
        Map<TopicPartition, Long> offsetsMapping = runOnContextAndAwait(
                this::clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping);
        commitAndAwait(offsetsMapping);
    }

    private void commitAndAwait(Map<TopicPartition, Long> offsetsMapping) {
        if (!offsetsMapping.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(offsetsMapping);
            CompletableFuture<Void> stage = consumer.commit(offsets)
                    .subscribeAsCompletionStage();
            try {
                stage.get(autoCommitInterval, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsets(Map<TopicPartition, Long> offsetsMapping) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : offsetsMapping.entrySet()) {
            map.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1L, null));
        }
        return map;
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
