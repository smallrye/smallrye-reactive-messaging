package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 * Will keep track of received messages and commit to the next offset after the latest
 * ACKed message in sequence. Will commit periodically as defined by `auto.commit.interval.ms` (default: 5000)
 *
 * This strategy mimics the behavior of the kafka consumer when `enable.auto.commit`
 * is `true`.
 *
 * The connector will be marked as unhealthy in the presence of any received record that has gone
 * too long without being processed as defined by `throttled.unprocessed-record-max-age.ms` (default: 60000).
 * If `throttled.unprocessed-record-max-age.ms` is set to less than or equal to 0 then will not
 * perform any health check (this might lead to running out of memory).
 *
 * This strategy guarantees at-least-once delivery even if the channel performs
 * asynchronous processing.
 *
 * To use set `commit-strategy` to `throttled`.
 */
public class KafkaThrottledLatestProcessedCommit implements KafkaCommitHandler {

    private static final Map<String, Map<Integer, TopicPartition>> TOPIC_PARTITIONS_CACHE = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetStore> offsetStores = new HashMap<>();

    private final KafkaConsumer<?, ?> consumer;
    private final KafkaSource<?, ?> source;
    private final int unprocessedRecordMaxAge;
    private final int autoCommitInterval;

    private volatile Context context;

    private long timerId = -1;

    private KafkaThrottledLatestProcessedCommit(KafkaConsumer<?, ?> consumer,
            KafkaSource<?, ?> source,
            int unprocessedRecordMaxAge,
            int autoCommitInterval) {
        this.consumer = consumer;
        this.source = source;
        this.unprocessedRecordMaxAge = unprocessedRecordMaxAge;
        this.autoCommitInterval = autoCommitInterval;
    }

    public static void clearCache() {
        TOPIC_PARTITIONS_CACHE.clear();
    }

    public static KafkaThrottledLatestProcessedCommit create(KafkaConsumer<?, ?> consumer,
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
        return new KafkaThrottledLatestProcessedCommit(consumer, source, unprocessedRecordMaxAge, autoCommitInterval);

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

    @Override
    public void partitionsAssigned(Context context, Set<TopicPartition> partitions) {
        this.context = context;

        offsetStores.clear();

        stopFlushAndCheckHealthTimer();

        if (!partitions.isEmpty()) {
            startFlushAndCheckHealthTimer();
        }
    }

    private void stopFlushAndCheckHealthTimer() {
        if (timerId != -1) {
            context.owner().cancelTimer(timerId);
            timerId = -1;
        }
    }

    private void startFlushAndCheckHealthTimer() {
        timerId = context
                .owner()
                .setTimer(autoCommitInterval, this::flushAndCheckHealth);
    }

    @Override
    public <K, V> IncomingKafkaRecord<K, V> received(IncomingKafkaRecord<K, V> record) {
        TopicPartition recordsTopicPartition = getTopicPartition(record);
        getOffsetStore(recordsTopicPartition).received(record.getOffset());

        return record;
    }

    private Map<TopicPartition, Long> clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping() {
        Map<TopicPartition, Long> offsetsMapping = new HashMap<>();

        offsetStores
                .forEach((topicPartition, value) -> value
                        .clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset()
                        .ifPresent(offset -> offsetsMapping.put(topicPartition, offset)));

        return offsetsMapping;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(final IncomingKafkaRecord<K, V> record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        context.runOnContext(v -> {
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
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsMapping
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            e -> new OffsetAndMetadata().setOffset(e.getValue() + 1L)));
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

        OptionalLong clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset() {
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
                    return OptionalLong.of(largestSequentialProcessedOffset);
                }
            }
            return OptionalLong.empty();
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
            super("Too Many Messages without Ack");
        }
    }

}
