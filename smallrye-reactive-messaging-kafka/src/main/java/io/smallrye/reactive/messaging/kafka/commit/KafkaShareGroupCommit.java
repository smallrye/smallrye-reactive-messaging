package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaShareConsumer;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
import io.smallrye.reactive.messaging.kafka.queues.ShareGroupAcknowledgement;
import io.vertx.mutiny.core.Vertx;

/**
 * Commit Handler for managing the commit lifecycle of messages consumed by a Kafka Share Group consumer.
 * It tracks in-progress messages, handles acknowledgments, and periodically renews locks on messages that are still being
 * processed.
 * It also checks for processing timeouts and reports failures if messages exceed the configured processing time.
 */
public class KafkaShareGroupCommit extends ContextHolder implements KafkaCommitHandler, AcknowledgementCommitCallback {

    private final KafkaShareConsumer<?, ?> consumer;
    private final BiConsumer<Throwable, Boolean> reportFailure;
    private final int renewInterval;
    private final int processingTimeout;

    private final Map<TopicPartition, Map<Long, TrackedRecord>> inProgress = new ConcurrentHashMap<>();

    private volatile long timerId = -1;

    public KafkaShareGroupCommit(KafkaShareConsumer<?, ?> consumer, Vertx vertx,
            int defaultTimeout, int autoCommitInterval, int processingTimeout,
            BiConsumer<Throwable, Boolean> reportFailure) {
        super(vertx, defaultTimeout);
        this.consumer = consumer;
        this.reportFailure = reportFailure;
        this.renewInterval = autoCommitInterval;
        this.processingTimeout = processingTimeout;
        consumer.runOnPollingThread(c -> {
            c.setAcknowledgementCommitCallback(this);
        }).subscribeAsCompletionStage();
    }

    boolean processingTimeoutEnabled() {
        return processingTimeout > 0;
    }

    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        record.injectMetadata(new ShareGroupAcknowledgement());
        TopicPartition topicPartition = TopicPartitions.getTopicPartition(record);
        IncomingKafkaRecordMetadata<?, ?> metadata = record.getMetadata(IncomingKafkaRecordMetadata.class).get();
        ConsumerRecord<?, ?> consumerRecord = metadata.getRecord();

        inProgress.computeIfAbsent(topicPartition, tp -> new ConcurrentHashMap<>())
                .put(record.getOffset(), new TrackedRecord(consumerRecord, System.currentTimeMillis()));

        if (processingTimeoutEnabled() && timerId < 0) {
            startRenewTimer();
        }
        return Uni.createFrom().item(record);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        IncomingKafkaRecordMetadata<?, ?> metadata = record.getMetadata(IncomingKafkaRecordMetadata.class).get();
        AcknowledgeType ackType = record.getMetadata(ShareGroupAcknowledgement.class)
                .map(ShareGroupAcknowledgement::getAcknowledgeType)
                .orElse(AcknowledgeType.ACCEPT);
        Uni<Void> ack = consumer.acknowledge((ConsumerRecord) metadata.getRecord(), ackType);
        return ack.emitOn(record::runOnMessageContext);
    }

    /**
     * Periodic action: renew acquisition locks for in-progress records
     * and check for processing timeouts.
     */
    @SuppressWarnings("unused")
    private void renewAndCheckTimeout(long ignored) {
        List<TrackedRecord> forRenew = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Map.Entry<TopicPartition, Map<Long, TrackedRecord>> partitionEntry : inProgress.entrySet()) {
            TopicPartition tp = partitionEntry.getKey();
            for (Map.Entry<Long, TrackedRecord> entry : partitionEntry.getValue().entrySet()) {
                long offset = entry.getKey();
                TrackedRecord tracked = entry.getValue();
                long elapsed = now - tracked.receivedAt;

                if (processingTimeoutEnabled() && elapsed > processingTimeout) {
                    // Processing timeout exceeded - report failure
                    log.shareGroupProcessingTimeout(tp.toString(), offset, elapsed / 1000, processingTimeout);
                    partitionEntry.getValue().remove(offset);
                    reportFailure.accept(
                            new ShareGroupProcessingTimeoutException(tp, offset, elapsed, processingTimeout),
                            false);
                } else {
                    forRenew.add(tracked);
                }
            }
        }

        if (!forRenew.isEmpty()) {
            consumer.runOnPollingThread(c -> {
                int acquisitionLockTimeoutMs = c.acquisitionLockTimeoutMs().orElse(0);
                int renewed = 0;
                for (TrackedRecord tracked : forRenew) {
                    // receivedAt + acquisitionLockTimeoutMs is the time when the lock would expire if not renewed
                    if (tracked.receivedAt + (long) acquisitionLockTimeoutMs < now + renewInterval) {
                        ConsumerRecord<?, ?> rec = tracked.record;
                        renewed++;
                        c.acknowledge((ConsumerRecord) rec, AcknowledgeType.RENEW);
                    }
                }
                log.shareGroupRenewingLocks(renewed);
            }).subscribe().with(
                    v -> startRenewTimer(),
                    f -> {
                        log.shareGroupAcknowledgementCommitFailed(f);
                        startRenewTimer();
                    });
        } else {
            startRenewTimer();
        }
    }

    private void startRenewTimer() {
        timerId = vertx.setTimer(renewInterval, x -> runOnContext(() -> this.renewAndCheckTimeout(x)));
    }

    private void stopRenewTimer() {
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
            timerId = -1;
        }
    }

    @Override
    public void terminate(boolean graceful) {
        stopRenewTimer();
        if (graceful) {
            waitForProcessing();
        }
    }

    private void waitForProcessing() {
        int attempts = renewInterval / 100;
        for (int i = 0; i < attempts; i++) {
            long pending = inProgress.values().stream().mapToLong(Map::size).sum();
            if (pending == 0) {
                return;
            }
            log.shareGroupPendingOffsets("all", pending + " records still in progress");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            // commitSyncAndAwait
            try {
                consumer.commit().await().atMost(Duration.ofMillis(getTimeoutInMillis()));
            } catch (Exception e) {
                log.shareGroupAcknowledgementCommitFailed(e);
            }
        }
    }

    /**
     * Callback for acknowledgement commit completion.
     * This is called by the consumer after an acknowledge() call completes,
     * with the offsets that were acknowledged and any exception that occurred during the commit.
     *
     * @param offsets A map of the offsets that this callback applies to.
     *
     * @param exception The exception thrown during processing of the request, or null if the acknowledgement completed
     *        successfully.
     */
    @Override
    public void onComplete(Map<TopicIdPartition, Set<Long>> offsets, Exception exception) {
        if (exception != null) {
            log.shareGroupAcknowledgementCommitFailed(exception);
        } else {
            log.shareGroupAcknowledgementCommitted(offsets.toString());
            offsets.forEach((tp, offsetSet) -> {
                TopicPartition topicPartition = tp.topicPartition();
                Map<Long, TrackedRecord> partitionRecords = inProgress.get(topicPartition);
                if (partitionRecords != null) {
                    offsetSet.forEach(partitionRecords::remove);
                    if (partitionRecords.isEmpty()) {
                        inProgress.remove(topicPartition);
                    } else {
                        log.shareGroupPendingOffsets(topicPartition.toString(), partitionRecords.keySet().toString());
                    }
                }
            });
        }
    }

    private record TrackedRecord(ConsumerRecord<?, ?> record, long receivedAt) {
    }

    public static class ShareGroupProcessingTimeoutException extends RuntimeException {
        private final TopicPartition topicPartition;
        private final long offset;

        public ShareGroupProcessingTimeoutException(TopicPartition topicPartition, long offset,
                long elapsedMs, long timeoutMs) {
            super(String.format(
                    "Record from topic-partition '%s' at offset %d has been processing for %d ms, exceeding timeout of %d ms",
                    topicPartition, offset, elapsedMs, timeoutMs));
            this.topicPartition = topicPartition;
            this.offset = offset;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public long getOffset() {
            return offset;
        }
    }
}
