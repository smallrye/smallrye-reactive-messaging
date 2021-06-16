package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.mutiny.core.Vertx;

/**
 * Will keep track of received messages and commit to the next offset after the latest
 * ACKed message in sequence. Will commit periodically as defined by `auto.commit.interval.ms` (default: 5000)
 * <p>
 * This strategy mimics the behavior of the kafka consumer when `enable.auto.commit`
 * is `true`.
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

    private static final Map<String, Map<Integer, TopicPartition>> TOPIC_PARTITIONS_CACHE = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetStore> offsetStores = new HashMap<>();

    private final String groupId;
    private final ReactiveKafkaConsumer<?, ?> consumer;
    private final KafkaSource<?, ?> source;
    private final int unprocessedRecordMaxAge;
    private final int autoCommitInterval;
    private volatile long timerId = -1;
    private final Collection<TopicPartition> assignments = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private KafkaThrottledLatestProcessedCommit(
            String groupId,
            Vertx vertx,
            ReactiveKafkaConsumer<?, ?> consumer,
            KafkaSource<?, ?> source,
            int unprocessedRecordMaxAge,
            int autoCommitInterval,
            int defaultTimeout) {
        super(vertx.getDelegate(), defaultTimeout);
        this.groupId = groupId;
        this.consumer = consumer;
        this.source = source;
        this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        this.autoCommitInterval = autoCommitInterval;
    }

    public static void clearCache() {
        TOPIC_PARTITIONS_CACHE.clear();
    }

    public static KafkaThrottledLatestProcessedCommit create(
            Vertx vertx,
            ReactiveKafkaConsumer<?, ?> consumer,
            String groupId,
            KafkaConnectorIncomingConfiguration config,
            KafkaSource<?, ?> source) {

        int unprocessedRecordMaxAge = config.getThrottledUnprocessedRecordMaxAgeMs();
        int autoCommitInterval = config.config()
                .getOptionalValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.class)
                .orElse(5000);
        int defaultTimeout = config.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                .orElse(60000);
        log.settingCommitInterval(groupId, autoCommitInterval);
        if (unprocessedRecordMaxAge <= 0) {
            log.disableThrottledCommitStrategyHealthCheck(groupId);
        } else {
            log.setThrottledCommitStrategyReceivedRecordMaxAge(groupId, unprocessedRecordMaxAge);
        }
        return new KafkaThrottledLatestProcessedCommit(groupId, vertx, consumer, source, unprocessedRecordMaxAge,
                autoCommitInterval, defaultTimeout);

    }

    private <K, V> TopicPartition getTopicPartition(IncomingKafkaRecord<K, V> record) {
        return TOPIC_PARTITIONS_CACHE
                .computeIfAbsent(record.getTopic(), topic -> new ConcurrentHashMap<>())
                .computeIfAbsent(record.getPartition(), partition -> new TopicPartition(record.getTopic(), partition));
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
        runOnContextAndAwait(() -> {
            assignments.removeAll(partitions);
            stopFlushAndCheckHealthTimer();
            return null;
        });

        // Remove all handled partitions that are not in the given list of partitions
        Callable<Tuple2<Map<TopicPartition, OffsetAndMetadata>, Boolean>> task = () -> {
            Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
            for (TopicPartition partition : new HashSet<>(offsetStores.keySet())) {
                if (partitions.contains(partition)) {
                    OffsetStore store = offsetStores.remove(partition);
                    long largestOffset = store.clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset();
                    if (largestOffset > -1) {
                        toCommit.put(partition, new OffsetAndMetadata(largestOffset + 1L, null));
                    }
                }
            }
            return Tuple2.of(toCommit, !offsetStores.isEmpty());
        };

        Tuple2<Map<TopicPartition, OffsetAndMetadata>, Boolean> result = runOnContextAndAwait(task);

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
        timerId = vertx.setTimer(autoCommitInterval, x -> {
            runOnContext(() -> this.flushAndCheckHealth(x));
        });

    }

    /**
     * Received a new record from Kafka.
     * This method is called from a Vert.x event loop.
     *
     * @param record the record
     * @param <K> the key
     * @param <V> the value
     * @return the record
     */
    @Override
    public <K, V> IncomingKafkaRecord<K, V> received(IncomingKafkaRecord<K, V> record) {
        TopicPartition recordsTopicPartition = getTopicPartition(record);
        offsetStores
                .computeIfAbsent(recordsTopicPartition, k -> new OffsetStore(k, unprocessedRecordMaxAge))
                .received(record.getOffset());

        if (timerId < 0) {
            startFlushAndCheckHealthTimer();
        }

        return record;
    }

    /**
     * Must be called from the event loop.
     *
     * @return the map of partition -> offset that can be committed.
     */
    private Map<TopicPartition, Long> clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping() {
        Map<TopicPartition, Long> offsetsMapping = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetStore> entry : offsetStores.entrySet()) {
            long offset = entry.getValue()
                    .clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset();
            if (offset > -1) {
                offsetsMapping.put(entry.getKey(), offset);
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
    public <K, V> CompletionStage<Void> handle(final IncomingKafkaRecord<K, V> record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Be sure to run on the right context. The context has been store during the message reception
        // or partition assignment.
        runOnContext(() -> {
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
                log.messageAckedForRevokedTopicPartition(record.getOffset(), groupId, topicPartition.toString());
            }
            future.complete(null);
        });
        return future;

    }

    /**
     * Always called from the event loop.
     *
     * @param timerId the timer id.
     */
    private void flushAndCheckHealth(long timerId) {
        Map<TopicPartition, Long> offsetsMapping = clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping();

        if (!offsetsMapping.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(offsetsMapping);
            // TODO Log commit failure.
            consumer.commit(offsets).subscribe().with(a -> this.startFlushAndCheckHealthTimer(),
                    f -> this.startFlushAndCheckHealthTimer());
        } else {
            this.startFlushAndCheckHealthTimer();
        }

        if (this.unprocessedRecordMaxAge > 0) {
            for (OffsetStore store : offsetStores.values()) {
                if (store.hasTooManyMessagesWithoutAck()) {
                    long lastOffset = store.getLastCommittedOffset();
                    String topic = store.topicPartition.topic();
                    int partition = store.topicPartition.partition();
                    TooManyMessagesWithoutAckException exception = new TooManyMessagesWithoutAckException(topic, partition,
                            lastOffset);
                    this.source.reportFailure(exception, true);
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
        private final Queue<OffsetReceivedAt> receivedOffsets = new LinkedList<>();
        private final Set<Long> processedOffsets = new HashSet<>();
        private final int unprocessedRecordMaxAge;
        private final AtomicLong unProcessedTotal = new AtomicLong();
        private long lastCommitted = -1;

        OffsetStore(TopicPartition topicPartition, int unprocessedRecordMaxAge) {
            this.topicPartition = topicPartition;
            this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        }

        long getLastCommittedOffset() {
            return lastCommitted;
        }

        void received(long offset) {
            this.receivedOffsets.offer(OffsetReceivedAt.received(offset));
            unProcessedTotal.incrementAndGet();
        }

        void processed(long offset) {
            final OffsetReceivedAt received = this.receivedOffsets.peek();
            if (received != null && received.getOffset() <= offset) {
                processedOffsets.add(offset);
            }
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
                    lastCommitted = largestSequentialProcessedOffset;
                    return largestSequentialProcessedOffset;
                }
            }
            return -1;
        }

        boolean hasTooManyMessagesWithoutAck() {
            if (receivedOffsets.isEmpty() || !isStillAssigned()) {
                return false;
            }
            OffsetReceivedAt peek = receivedOffsets.peek();
            long time = peek == null ? 0 : (System.currentTimeMillis() - peek.getReceivedAt());
            if (time > unprocessedRecordMaxAge) {
                log.receivedTooManyMessagesWithoutAcking(topicPartition.toString(), unProcessedTotal.get(), lastCommitted);
                return true;
            }
            return false;
        }

        private boolean isStillAssigned() {
            // If the topic/partition is not assigned to us already, the store will be cleared eventually.
            return assignments.contains(topicPartition);
        }

        long getUnprocessedCount() {
            return unProcessedTotal.get();
        }
    }

    public static class TooManyMessagesWithoutAckException extends NoStackTraceThrowable {
        public TooManyMessagesWithoutAckException(String topic, int partition, long lastOffset) {
            super("Too Many Messages without acknowledgement in topic " + topic + " (partition:" + partition
                    + "), last committed offset is " + lastOffset);
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
}
