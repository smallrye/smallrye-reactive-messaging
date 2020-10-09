package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

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

    private final KafkaConsumer<?, ?> consumer;
    private final KafkaSource<?, ?> source;
    private final int unprocessedRecordMaxAge;
    private final int autoCommitInterval;
    private volatile long timerId = -1;

    private KafkaThrottledLatestProcessedCommit(
            Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            KafkaSource<?, ?> source,
            int unprocessedRecordMaxAge,
            int autoCommitInterval) {
        super(vertx);
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
            KafkaConsumer<?, ?> consumer,
            String groupId,
            KafkaConnectorIncomingConfiguration config,
            KafkaSource<?, ?> source) {

        int unprocessedRecordMaxAge = config.getThrottledUnprocessedRecordMaxAgeMs();
        int autoCommitInterval = config.config()
                .getOptionalValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.class)
                .orElse(5000);
        log.settingCommitInterval(groupId, autoCommitInterval);
        if (unprocessedRecordMaxAge <= 0) {
            log.disableThrottledCommitStrategyHealthCheck(groupId);
        } else {
            log.setThrottledCommitStrategyReceivedRecordMaxAge(groupId, unprocessedRecordMaxAge);
        }
        return new KafkaThrottledLatestProcessedCommit(vertx, consumer, source, unprocessedRecordMaxAge,
                autoCommitInterval);

    }

    private <K, V> TopicPartition getTopicPartition(IncomingKafkaRecord<K, V> record) {
        return TOPIC_PARTITIONS_CACHE
                .computeIfAbsent(record.getTopic(), topic -> new ConcurrentHashMap<>())
                .computeIfAbsent(record.getPartition(), partition -> new TopicPartition(record.getTopic(), partition));
    }

    private OffsetStore getOffsetStore(TopicPartition topicPartition) {
        return offsetStores
                .computeIfAbsent(topicPartition, k -> new OffsetStore(k, unprocessedRecordMaxAge));
    }

    /**
     * New partitions are assigned.
     * This method is called from a Vert.x event loop (the one used by the Kafka client)
     *
     * @param partitions the partitions
     */
    @Override
    public void partitionsAssigned(Set<TopicPartition> partitions) {
        stopFlushAndCheckHealthTimer();

        if (!partitions.isEmpty() || !offsetStores.isEmpty()) {
            startFlushAndCheckHealthTimer();
        }
    }

    /**
     * Revoked partitions.
     * This method is called from a Vert.x event loop (the one used by the Kafka client)
     *
     * @param partitions the partitions that we will no longer receive
     */
    @Override
    public void partitionsRevoked(Set<TopicPartition> partitions) {
        stopFlushAndCheckHealthTimer();

        // Remove all handled partitions that are in the given list of revoked partitions
        Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
        for (TopicPartition partition : partitions) {
            OffsetStore store = offsetStores.remove(partition);
            if (store != null) {
                long largestOffset = store.clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset();
                if (largestOffset > -1) {
                    toCommit.put(partition, new OffsetAndMetadata(largestOffset + 1L, null));
                }
            }
        }

        if (!toCommit.isEmpty()) {
            consumer.getDelegate().commit(toCommit);
        }

        if (!offsetStores.isEmpty()) {
            startFlushAndCheckHealthTimer();
        }
    }

    private void stopFlushAndCheckHealthTimer() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
            timerId = -1;
        }
    }

    private void startFlushAndCheckHealthTimer() {
        timerId = vertx.setTimer(autoCommitInterval, this::flushAndCheckHealth);
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
        getOffsetStore(recordsTopicPartition).received(record.getOffset());

        if (timerId < 0) {
            startFlushAndCheckHealthTimer();
        }

        return record;
    }

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
            offsetStores
                    .get(getTopicPartition(record))
                    .processed(record.getOffset());
            future.complete(null);
        });
        return future;

    }

    private void flushAndCheckHealth(long timerId) {
        Map<TopicPartition, Long> offsetsMapping = clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping();

        if (!offsetsMapping.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(offsetsMapping);
            consumer.getDelegate().commit(offsets, a -> this.startFlushAndCheckHealthTimer());
        } else {
            this.startFlushAndCheckHealthTimer();
        }

        if (this.unprocessedRecordMaxAge > 0) {
            offsetStores
                    .values()
                    .stream()
                    .filter(OffsetStore::hasTooManyMessagesWithoutAck)
                    .forEach(o -> this.source.reportFailure(new TooManyMessagesWithoutAckException()));
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

    private static class OffsetStore {

        private final TopicPartition topicPartition;
        private final Queue<OffsetReceivedAt> receivedOffsets = new LinkedList<>();
        private final Set<Long> processedOffsets = new HashSet<>();
        private final int unprocessedRecordMaxAge;
        private long unProcessedTotal = 0L;

        OffsetStore(TopicPartition topicPartition, int unprocessedRecordMaxAge) {
            this.topicPartition = topicPartition;
            this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        }

        void received(long offset) {
            this.receivedOffsets.offer(OffsetReceivedAt.received(offset));
            unProcessedTotal++;
        }

        void processed(long offset) {
            if (!this.receivedOffsets.isEmpty() && this.receivedOffsets.peek().getOffset() <= offset) {
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
                    unProcessedTotal--;
                    largestSequentialProcessedOffset = receivedOffsets.poll().getOffset();
                }

                if (largestSequentialProcessedOffset > -1) {
                    return largestSequentialProcessedOffset;
                }
            }
            return -1;
        }

        boolean hasTooManyMessagesWithoutAck() {
            if (receivedOffsets.isEmpty()) {
                return false;
            }
            if (System.currentTimeMillis() - receivedOffsets.peek().getReceivedAt() > unprocessedRecordMaxAge) {
                log.receivedTooManyMessagesWithoutAcking(topicPartition.toString(), unProcessedTotal);
                return true;
            }
            return false;
        }
    }

    public static class TooManyMessagesWithoutAckException extends Exception {
        public TooManyMessagesWithoutAckException() {
            super("Too Many Messages without acknowledgement");
        }
    }

    @Override
    public void terminate() {
        commitAllAndAwait();
        offsetStores.clear();
        stopFlushAndCheckHealthTimer();
    }

    private void commitAllAndAwait() {
        Map<TopicPartition, Long> offsetsMapping = clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping();
        commitAndAwait(offsetsMapping);
    }

    private void commitAndAwait(Map<TopicPartition, Long> offsetsMapping) {
        if (!offsetsMapping.isEmpty()) {

            CompletableFuture<Void> stage = new CompletableFuture<>();
            Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(offsetsMapping);
            consumer.getDelegate().commit(offsets, new Handler<AsyncResult<Map<TopicPartition, OffsetAndMetadata>>>() {
                @Override
                public void handle(
                        AsyncResult<Map<TopicPartition, OffsetAndMetadata>> event) {
                    if (event.failed()) {
                        stage.completeExceptionally(event.cause());
                    } else {
                        stage.complete(null);
                    }
                }
            });

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
