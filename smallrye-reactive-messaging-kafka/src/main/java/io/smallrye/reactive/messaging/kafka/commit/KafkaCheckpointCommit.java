package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
import io.smallrye.reactive.messaging.providers.helpers.VertxContext;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.mutiny.core.Vertx;

/**
 * Commit handler for checkpointing processing state persisted in a state store
 * <p>
 * Instead of committing topic-partition offsets back to Kafka, checkpointing commit handlers persist and restore offsets on an
 * external store.
 * It associates a {@link ProcessingState} with a topic-partition offset, and lets the processing resume from the checkpointed
 * state.
 * <p>
 * This abstract implementation holds a local map of {@link ProcessingState} per topic-partition,
 * and ensures it is accessed on the captured Vert.x context.
 * <p>
 */
@Experimental("Experimental API")
public class KafkaCheckpointCommit extends ContextHolder implements KafkaCommitHandler {

    private final Map<TopicPartition, CheckpointState<?>> checkpointStateMap = new ConcurrentHashMap<>();

    private volatile long timerId = -1;

    private final int autoCommitInterval;

    private final KafkaConsumer<?, ?> consumer;
    private final CheckpointStateStore stateStore;

    private final BiConsumer<Throwable, Boolean> reportFailure;
    private final String consumerId;
    private final int unsyncedStateMaxAge;

    public KafkaCheckpointCommit(Vertx vertx,
            KafkaConsumer<?, ?> consumer,
            CheckpointStateStore stateStore,
            BiConsumer<Throwable, Boolean> reportFailure,
            int autoCommitInterval,
            int unsynchedStateMaxAge,
            int defaultTimeout) {
        super(vertx, defaultTimeout);
        this.consumer = consumer;
        this.consumerId = (String) consumer.configuration().get(ConsumerConfig.CLIENT_ID_CONFIG);
        this.stateStore = stateStore;
        this.reportFailure = reportFailure;
        this.autoCommitInterval = autoCommitInterval;
        this.unsyncedStateMaxAge = unsynchedStateMaxAge;
        if (unsynchedStateMaxAge <= 0) {
            log.disableCheckpointCommitStrategyHealthCheck(consumerId);
        } else {
            log.setCheckpointCommitStrategyUnsyncedStateMaxAge(consumerId, unsynchedStateMaxAge);
        }
    }

    @ApplicationScoped
    @Identifier(Strategy.CHECKPOINT)
    public static class Factory implements KafkaCommitHandler.Factory {

        Instance<CheckpointStateStore.Factory> stateStoreFactory;

        @Inject
        public Factory(@Any Instance<CheckpointStateStore.Factory> stateStoreFactory) {
            this.stateStoreFactory = stateStoreFactory;
        }

        @Override
        public KafkaCommitHandler create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure) {
            String groupId = (String) consumer.configuration().get(ConsumerConfig.GROUP_ID_CONFIG);
            int defaultTimeout = config.config()
                    .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class)
                    .orElse(60000);
            int autoCommitInterval = config.config()
                    .getOptionalValue(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Integer.class)
                    .orElse(5000);
            log.settingCommitInterval(groupId, autoCommitInterval);
            String stateStoreIdentifier = config.getCheckpointStateStore().orElseGet(() -> {
                log.checkpointDefaultStateStore();
                return "file";
            });
            CheckpointStateStore.Factory factory = stateStoreFactory.select(Identifier.Literal.of(stateStoreIdentifier)).get();
            Class<?> stateType = config.getCheckpointStateType()
                    .map(fqcn -> {
                        try {
                            return (Class<?>) Utils.loadClass(fqcn, Object.class);
                        } catch (ClassNotFoundException e) {
                            log.checkpointStateTypeNotFound(config.getChannel(), fqcn);
                            return null;
                        }
                    })
                    .orElse(null);
            CheckpointStateStore stateStore = factory.create(config, vertx, consumer, stateType);
            return new KafkaCheckpointCommit(vertx, consumer, stateStore, reportFailure, autoCommitInterval,
                    config.getCheckpointUnsyncedStateMaxAgeMs(),
                    defaultTimeout);
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
        if (!checkpointStateMap.isEmpty()) {
            timerId = vertx.setTimer(autoCommitInterval, x -> runOnContext(this::flushAndCheckHealth));
        }
    }

    private void flushAndCheckHealth() {
        this.persistProcessingState(checkpointStateMap)
                .onItemOrFailure().invoke(() -> {
                    startFlushAndCheckHealthTimer();
                    checkHealth();
                }).subscribe().with(unused -> {
                }, unused -> {
                });
    }

    private void checkHealth() {
        if (this.unsyncedStateMaxAge > 0) {
            for (Map.Entry<TopicPartition, CheckpointState<?>> state : checkpointStateMap.entrySet()) {
                TopicPartition tp = state.getKey();
                CheckpointState<?> checkpointState = state.getValue();
                long elapsed = checkpointState.millisSinceLastPersistedOffset();
                boolean waitedTooLong = elapsed > unsyncedStateMaxAge;
                if (waitedTooLong) {
                    LastStateStoredTooLongAgoException exception = new LastStateStoredTooLongAgoException(tp,
                            elapsed / 1000,
                            checkpointState.getProcessingState().getOffset(),
                            checkpointState.persistedAt.getOffset());
                    log.warnf(exception, exception.getMessage());
                    this.reportFailure.accept(exception, true);
                }
            }
        }
    }

    /**
     * Called on Vert.x event loop
     */
    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        TopicPartition tp = TopicPartitions.getTopicPartition(record);
        CheckpointState<?> state = checkpointStateMap.get(tp);
        if (state != null) {
            state.receivedRecord();
            record.injectMetadata(new DefaultCheckpointMetadata<>(tp, record.getOffset(), state));
        }
        if (timerId < 0) {
            startFlushAndCheckHealthTimer();
        }
        return Uni.createFrom().item(record);
    }

    /**
     * Called on Vert.x event loop
     */
    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        return Uni.createFrom().completionStage(VertxContext.runOnEventLoopContext(context.getDelegate(), f -> {
            TopicPartition tp = TopicPartitions.getTopicPartition(record);
            CheckpointState<?> checkpointState = checkpointStateMap.get(tp);
            DefaultCheckpointMetadata<?> metadata = DefaultCheckpointMetadata.fromMessage(record);
            if (metadata != null && metadata.getCheckpointState().equals(checkpointState)) {
                ProcessingState<?> newState = metadata.getNext();
                checkpointState.processedRecord();
                if (!ProcessingState.isEmptyOrNull(newState)) {
                    if (metadata.isPersistOnAck()) {
                        this.persistProcessingState(Map.of(tp, checkpointState))
                                .onFailure().recoverWithNull()
                                .emitOn(record::runOnMessageContext)
                                .subscribe().with(unused -> f.complete(null), f::completeExceptionally);
                        return;
                    }
                }
            } else {
                log.acknowledgementFromRevokedTopicPartition(record.getOffset(), tp, consumerId, checkpointStateMap.keySet());
            }
            record.runOnMessageContext(() -> f.complete(null));
        }));
    }

    /**
     *
     * @param graceful {@code true} if it is a graceful shutdown
     */
    @Override
    public void terminate(boolean graceful) {
        if (graceful) {
            long stillUnprocessed = waitForProcessing();
            if (stillUnprocessed > 0) {
                log.messageStillUnprocessedAfterTimeout(stillUnprocessed);
            }
        }

        removeFromState(checkpointStateMap.keySet())
                .chain(this::persistProcessingState)
                .runSubscriptionOn(this::runOnContext)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
        stateStore.close();
    }

    private long waitForProcessing() {
        int attempt = autoCommitInterval / 100;
        for (int i = 0; i < attempt; i++) {
            long sum = checkpointStateMap.values().stream().map(CheckpointState::getUnprocessedRecords).mapToLong(l -> l).sum();
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

        return checkpointStateMap.values().stream().map(CheckpointState::getUnprocessedRecords).mapToLong(l -> l).sum();
    }

    /**
     *
     * @param partitions assigned partitions
     */
    @Override
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        stopFlushAndCheckHealthTimer();
        if (partitions.isEmpty()) {
            return;
        }

        Map<TopicPartition, ProcessingState<?>> fetchedStates = Uni.createFrom()
                .deferred(() -> stateStore.fetchProcessingState(partitions)
                        .onItem()
                        .invoke(fetched -> log.checkpointPartitionsAssigned(consumerId, partitions, fetched.toString()))
                        .onFailure().invoke(f -> log.failedCheckpointPartitionsAssigned(consumerId, partitions, f))
                        .invoke(fetched -> {
                            for (TopicPartition tp : partitions) {
                                ProcessingState<?> state = fetched.get(tp);
                                checkpointStateMap.put(tp, new CheckpointState<>(tp, state));
                            }
                            startFlushAndCheckHealthTimer();
                        }))
                .runSubscriptionOn(this::runOnContext)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));

        Consumer<?, ?> kafkaConsumer = consumer.unwrap();
        for (Map.Entry<TopicPartition, ? extends ProcessingState<?>> entry : fetchedStates.entrySet()) {
            ProcessingState<?> state = entry.getValue();
            kafkaConsumer.seek(entry.getKey(), state != null ? state.getOffset() : 0L);
        }
    }

    private Uni<Map<TopicPartition, CheckpointState<?>>> removeFromState(Collection<TopicPartition> partitions) {
        return Uni.createFrom().emitter(e -> {
            stopFlushAndCheckHealthTimer();
            Map<TopicPartition, CheckpointState<?>> toRemove = new HashMap<>(checkpointStateMap);
            checkpointStateMap.keySet().removeAll(partitions);
            toRemove.keySet().removeAll(partitions);
            e.complete(toRemove);
        });
    }

    /**
     *
     * @param partitions revoked partitions
     */
    @Override
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        removeFromState(partitions)
                .invoke(revoked -> log.checkpointPartitionsRevoked(consumerId, partitions, revoked.toString()))
                .chain(this::persistProcessingState)
                .invoke(this::startFlushAndCheckHealthTimer)
                .runSubscriptionOn(this::runOnContext)
                .await().atMost(Duration.ofMillis(getTimeoutInMillis()));
    }

    Uni<Void> persistProcessingState(Map<TopicPartition, CheckpointState<?>> stateMap) {
        Map<TopicPartition, ProcessingState<?>> map = new HashMap<>();
        for (Map.Entry<TopicPartition, CheckpointState<?>> entry : stateMap.entrySet()) {
            CheckpointState<?> checkpointState = entry.getValue();
            if (checkpointState.hasUnsyncedOffset()) {
                map.put(entry.getKey(), checkpointState.getProcessingState());
            }
        }
        if (map.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return stateStore.persistProcessingState(map)
                .onItem().invoke(() -> map.forEach((tp, state) -> checkpointStateMap.computeIfPresent(tp,
                        (t, s) -> s.withPersistedAt(OffsetPersistedAt.persisted(state.getOffset())))))
                .onItem().invoke(() -> log.checkpointPersistedState(consumerId, checkpointStateMap.toString()))
                .onFailure().invoke(t -> log.checkpointFailedPersistingState(consumerId, checkpointStateMap.toString(), t));
    }

    private static class OffsetPersistedAt {
        private final long offset;
        private final long persistedAt;

        public static OffsetPersistedAt NOT_PERSISTED = new OffsetPersistedAt(-1, -1);

        public static OffsetPersistedAt persisted(long offset) {
            return new OffsetPersistedAt(offset, System.currentTimeMillis());
        }

        private OffsetPersistedAt(long offset, long persistedAt) {
            this.offset = offset;
            this.persistedAt = persistedAt;
        }

        public boolean notPersisted() {
            return NOT_PERSISTED.equals(this);
        }

        public long getOffset() {
            return offset;
        }

        public long getPersistedAt() {
            return persistedAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            OffsetPersistedAt that = (OffsetPersistedAt) o;
            return offset == that.offset && persistedAt == that.persistedAt;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, persistedAt);
        }

        @Override
        public String toString() {
            return "OffsetPersistedAt{" +
                    "offset=" + offset +
                    ", persistedAt=" + persistedAt +
                    '}';
        }
    }

    public static class LastStateStoredTooLongAgoException extends NoStackTraceThrowable {

        public LastStateStoredTooLongAgoException(TopicPartition topic, long time, long currentStateOffset,
                long lastStoredOffset) {
            super(String.format("Latest processing state for topic-partition `%s` persisted %d seconds ago. " +
                    "At the moment latest registered local processing state is for offset %d. " +
                    "The last offset for which a state is successfully persisted was %d.",
                    topic, time, currentStateOffset, lastStoredOffset));
        }
    }

    public static class CheckpointState<T> {

        private final TopicPartition topicPartition;
        private final long createdTimestamp;
        private final AtomicLong received;
        private final AtomicLong processed;
        private volatile ProcessingState<T> processingState;
        private volatile OffsetPersistedAt persistedAt;

        private CheckpointState(TopicPartition topicPartition,
                ProcessingState<T> processingState,
                OffsetPersistedAt persistedAt) {
            this.topicPartition = topicPartition;
            this.createdTimestamp = System.currentTimeMillis();
            this.processingState = processingState;
            this.persistedAt = persistedAt;
            this.processed = new AtomicLong(0);
            this.received = new AtomicLong(0);
        }

        public CheckpointState(TopicPartition topicPartition) {
            this(topicPartition, (ProcessingState<T>) ProcessingState.EMPTY_STATE);
        }

        public CheckpointState(TopicPartition topicPartition, ProcessingState<T> processingState) {
            this(topicPartition, ProcessingState.getOrEmpty(processingState), OffsetPersistedAt.NOT_PERSISTED);
        }

        public CheckpointState<T> withPersistedAt(OffsetPersistedAt offsetPersistedAt) {
            this.persistedAt = offsetPersistedAt;
            return this;
        }

        public synchronized ProcessingState<T> transformState(Supplier<ProcessingState<T>> initialStateSupplier,
                Function<ProcessingState<T>, ProcessingState<T>> transform) {
            ProcessingState<T> previousState = ProcessingState.isEmptyOrNull(processingState) ? initialStateSupplier.get()
                    : processingState;
            processingState = transform.apply(previousState);
            return previousState;
        }

        public TopicPartition getTopicPartition() {
            return topicPartition;
        }

        public ProcessingState<T> getProcessingState() {
            return processingState;
        }

        public OffsetPersistedAt getPersistedAt() {
            return persistedAt;
        }

        public void receivedRecord() {
            received.incrementAndGet();
        }

        public void processedRecord() {
            processed.incrementAndGet();
        }

        public long getUnprocessedRecords() {
            return received.get() - processed.get();
        }

        public long millisSinceLastPersistedOffset() {
            // state never persisted, count the time passed since local state store
            if (persistedAt.notPersisted() && received.get() > 0) {
                return System.currentTimeMillis() - createdTimestamp;
            } else if (hasUnsyncedOffset()) {
                return System.currentTimeMillis() - persistedAt.getPersistedAt();
            } else {
                return -1;
            }
        }

        public boolean hasUnsyncedOffset() {
            return !ProcessingState.isEmptyOrNull(processingState) && processingState.getOffset() > persistedAt.getOffset();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CheckpointState<?> that = (CheckpointState<?>) o;
            return createdTimestamp == that.createdTimestamp && topicPartition.equals(that.topicPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicPartition, createdTimestamp);
        }

        @Override
        public String toString() {
            return "CheckpointState{" +
                    "topicPartition=" + topicPartition +
                    ", createdTimestamp=" + createdTimestamp +
                    ", received=" + received +
                    ", processed=" + processed +
                    ", processingState=" + processingState +
                    ", persistedAt=" + persistedAt +
                    '}';
        }
    }

}
