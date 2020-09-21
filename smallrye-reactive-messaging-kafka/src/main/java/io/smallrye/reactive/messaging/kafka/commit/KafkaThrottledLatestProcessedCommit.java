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
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 * Will keep track of received messages and commit to the next offset after the latest
 * ACKed message in sequence. Will only commit every 5 seconds.
 *
 * This strategy mimics the behavior of the kafka consumer when `enable.auto.commit`
 * is `true`.
 *
 * If too many received messages are not ACKed then the KafkaSource will be marked
 * as unhealthy. "Too many" is defined as a power of two value greater than or equal
 * to `max.poll.records` times 2.
 *
 * This strategy guarantees at-least-once delivery even if the channel performs
 * asynchronous processing.
 *
 * To use set `commit-strategy` to `throttled`.
 */
public class KafkaThrottledLatestProcessedCommit implements KafkaCommitHandler {

    private static final long THROTTLE_TIME_IN_MILLIS = 5_000L;
    private static final Map<String, Map<Integer, TopicPartition>> TOPIC_PARTITIONS_CACHE = new ConcurrentHashMap<>();

    private final Map<TopicPartition, OffsetStore> offsetStores = new HashMap<>();

    private final KafkaConsumer<?, ?> consumer;
    private final KafkaSource<?, ?> source;
    private final int maxReceivedWithoutAckAllowed;

    private volatile Context context;
    private long nextCommitTime;

    private KafkaThrottledLatestProcessedCommit(KafkaConsumer<?, ?> consumer,
            KafkaSource<?, ?> source,
            int maxReceivedWithoutAckAllowed) {
        this.consumer = consumer;
        this.source = source;
        this.maxReceivedWithoutAckAllowed = maxReceivedWithoutAckAllowed;
    }

    private static int getNextPowerOfTwoEqualOrGreater(int v) {
        if (v <= 0)
            return 1;
        v--;
        v |= v >>> 1;
        v |= v >>> 2;
        v |= v >>> 4;
        v |= v >>> 8;
        v |= v >>> 16;
        v++;
        return v;
    }

    public static void clearCache() {
        TOPIC_PARTITIONS_CACHE.clear();
    }

    public static KafkaThrottledLatestProcessedCommit create(KafkaConsumer<?, ?> consumer,
            Map<String, String> config,
            KafkaSource<?, ?> source) {

        int maxPollRecords = Integer.parseInt(config.getOrDefault(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500"));
        int maxReceivedWithoutAckAllowed = getNextPowerOfTwoEqualOrGreater(maxPollRecords * 2);
        log.settingMaxReceivedWithoutAckAllowed(config.get(ConsumerConfig.GROUP_ID_CONFIG), maxReceivedWithoutAckAllowed);
        return new KafkaThrottledLatestProcessedCommit(consumer, source, maxReceivedWithoutAckAllowed);

    }

    private <K, V> TopicPartition getTopicPartition(IncomingKafkaRecord<K, V> record) {
        return TOPIC_PARTITIONS_CACHE
                .computeIfAbsent(record.getTopic(), topic -> new ConcurrentHashMap<>())
                .computeIfAbsent(record.getPartition(), partition -> new TopicPartition(record.getTopic(), partition));
    }

    private OffsetStore getOffsetStore(TopicPartition topicPartition) {
        return offsetStores
                .computeIfAbsent(topicPartition, t -> new OffsetStore(t, this.maxReceivedWithoutAckAllowed));
    }

    @Override
    public void partitionsAssigned(Context context, Set<TopicPartition> partitions) {
        this.context = context;

        offsetStores.clear();

        resetNextCommitTime();
    }

    @Override
    public <K, V> IncomingKafkaRecord<K, V> received(IncomingKafkaRecord<K, V> record) {
        TopicPartition recordsTopicPartition = getTopicPartition(record);
        try {
            getOffsetStore(recordsTopicPartition).received(record.getOffset());
        } catch (TooManyMessagesWithoutAckException ex) {
            this.source.reportFailure(ex);
        }

        return record;
    }

    private void resetNextCommitTime() {
        this.nextCommitTime = System.currentTimeMillis() + THROTTLE_TIME_IN_MILLIS;
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

            if (System.currentTimeMillis() > this.nextCommitTime) {
                resetNextCommitTime();
                Map<TopicPartition, Long> offsetsMapping = clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffsetMapping();

                if (!offsetsMapping.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> offsets = offsetsMapping
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey,
                                    e -> new OffsetAndMetadata().setOffset(e.getValue() + 1L)));
                    consumer.getDelegate().commit(offsets, a -> future.complete(null));

                    return;
                }
            }
            future.complete(null);
        });
        return future;

    }

    private static class OffsetStore {

        private final TopicPartition topicPartition;
        private final Queue<Long> receivedOffsets = new LinkedList<>();
        private final Set<Long> processedOffsets = new HashSet<>();
        private final int maxReceivedWithoutAckAllowed;
        private final int maxReceivedWithoutAckAllowedMinusOne;
        private long unProcessedTotal = 0L;

        OffsetStore(TopicPartition topicPartition, int maxReceivedWithoutAckAllowed) {
            this.topicPartition = topicPartition;
            this.maxReceivedWithoutAckAllowed = maxReceivedWithoutAckAllowed;
            this.maxReceivedWithoutAckAllowedMinusOne = maxReceivedWithoutAckAllowed - 1;
        }

        void received(long offset) throws TooManyMessagesWithoutAckException {
            this.receivedOffsets.offer(offset);
            unProcessedTotal++;
            if (unProcessedTotal >= maxReceivedWithoutAckAllowed &&
                    (unProcessedTotal & maxReceivedWithoutAckAllowedMinusOne) == 0) {
                log.receivedTooManyMessagesWithoutAcking(topicPartition.toString(), unProcessedTotal);
                throw new TooManyMessagesWithoutAckException();
            }
        }

        void processed(long offset) {
            if (!this.receivedOffsets.isEmpty() && this.receivedOffsets.peek() <= offset) {
                processedOffsets.add(offset);
            }
        }

        OptionalLong clearLesserSequentiallyProcessedOffsetsAndReturnLargestOffset() {
            if (!processedOffsets.isEmpty()) {
                long largestSequentialProcessedOffset = -1;
                while (!receivedOffsets.isEmpty()) {
                    if (!processedOffsets.remove(receivedOffsets.peek())) {
                        break;
                    }
                    unProcessedTotal--;
                    largestSequentialProcessedOffset = receivedOffsets.poll();
                }

                if (largestSequentialProcessedOffset > -1) {
                    return OptionalLong.of(largestSequentialProcessedOffset);
                }
            }
            return OptionalLong.empty();
        }
    }

    public static class TooManyMessagesWithoutAckException extends Exception {
        public TooManyMessagesWithoutAckException() {
            super("Too Many Messages without Ack");
        }
    }

}
