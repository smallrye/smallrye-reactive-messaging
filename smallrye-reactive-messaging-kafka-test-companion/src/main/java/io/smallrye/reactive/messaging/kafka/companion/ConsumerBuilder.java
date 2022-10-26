package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.waitFor;
import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.until;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Kafka consumer wrapper for creating tasks ({@link ConsumerTask}) that consume records.
 * <p>
 * The wrapped Kafka consumer is created lazily when a {@link ConsumerTask} is created and started.
 * Until the consumer is created builder methods prefixed with {@code with-} act to provide additional configuration.
 * <p>
 * The created consumer is closed automatically when the {@code task} is terminated.
 * The consumer uses an underlying {@code ExecutorService} for polling records.
 * As Kafka consumer is not thread-safe, other operations accessing the underlying consumer are also run inside the
 * {@code ExecutorService}
 *
 * @param <K> The record key type
 * @param <V> The record value type
 */
public class ConsumerBuilder<K, V> implements ConsumerRebalanceListener, Closeable {

    private final static Logger LOGGER = Logger.getLogger(ConsumerBuilder.class);

    /**
     * Map of properties to use for creating Kafka consumer
     */
    private final Map<String, Object> props;

    /**
     * Function to create the {@link KafkaConsumer}
     */
    private final Function<Map<String, Object>, KafkaConsumer<K, V>> consumerCreator;

    /**
     * Kafka consumer
     */
    private KafkaConsumer<K, V> kafkaConsumer;

    /**
     * Executor service to use polling records from Kafka
     */
    private ScheduledExecutorService consumerExecutor;

    /**
     * Function to call to commitAsync return partitions
     */
    private Function<ConsumerRecord<K, V>, Map<TopicPartition, OffsetAndMetadata>> commitAsyncFunction;

    /**
     * Function to call to commitSync return partitions
     */
    private Function<ConsumerRecord<K, V>, Map<TopicPartition, OffsetAndMetadata>> commitSyncFunction;

    /**
     * Callback for commitAsync
     */
    private OffsetCommitCallback commitAsyncCallback;

    /**
     * Callback for partitions assigned
     */
    private Consumer<Collection<TopicPartition>> onPartitionsAssigned;

    /**
     * Callback for partitions revoked
     */
    private Consumer<Collection<TopicPartition>> onPartitionsRevoked;

    /**
     * Callback to call onTermination
     */
    private BiConsumer<KafkaConsumer<K, V>, Throwable> onTermination = this::terminate;

    /**
     * Duration for default api timeout
     */
    private final Duration kafkaApiTimeout;

    /**
     * Duration to pass to {@link KafkaConsumer#poll(Duration)}
     */
    private Duration pollTimeout = Duration.ofMillis(10);

    /**
     * Polling state
     */
    private final AtomicBoolean polling = new AtomicBoolean(false);

    /**
     * Set of current assignments
     */
    private final Set<TopicPartition> assignment = new HashSet<>();

    /**
     * Creates a new {@link ConsumerBuilder}.
     * <p>
     * Note that on producer creation {@link Deserializer#configure} methods will NOT be called.
     *
     * @param props the initial properties for consumer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keyDeser the deserializer for record keys
     * @param valueDeser the deserializer for record values
     */
    public ConsumerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            Deserializer<K> keyDeser, Deserializer<V> valueDeser) {
        this.props = props;
        this.kafkaApiTimeout = kafkaApiTimeout;
        this.consumerCreator = p -> new KafkaConsumer<>(p, keyDeser, valueDeser);
    }

    /**
     * Creates a new {@link ConsumerBuilder}.
     * <p>
     * On consumer creation, key and value deserializers will be created and {@link Deserializer#configure} methods will be
     * called.
     *
     * @param props the initial properties for consumer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keyDeserializerType the deserializer class name for record keys
     * @param valueDeserializerType the deserializer class name for record values
     */
    public ConsumerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            String keyDeserializerType, String valueDeserializerType) {
        this.props = props;
        this.kafkaApiTimeout = kafkaApiTimeout;
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerType);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerType);
        this.consumerCreator = KafkaConsumer::new;
    }

    private synchronized KafkaConsumer<K, V> getOrCreateConsumer() {
        if (this.kafkaConsumer == null) {
            this.kafkaConsumer = consumerCreator.apply(props);
        }
        return this.kafkaConsumer;
    }

    private synchronized ScheduledExecutorService getOrCreateExecutor() {
        if (this.consumerExecutor == null) {
            this.consumerExecutor = Executors.newSingleThreadScheduledExecutor(c -> new Thread(c, "consumer-" + clientId()));
        }
        return this.consumerExecutor;
    }

    /**
     * @return the underlying {@link KafkaConsumer}, may be {@code null} if producer is not yet created.
     */
    public KafkaConsumer<K, V> unwrap() {
        return kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.infof("%s revoked partitions %s", clientId(), partitions);
        assignment.removeAll(partitions);
        if (onPartitionsRevoked != null) {
            onPartitionsRevoked.accept(partitions);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.infof("%s assigned partitions %s", clientId(), partitions);
        assignment.addAll(partitions);
        if (onPartitionsAssigned != null) {
            onPartitionsAssigned.accept(partitions);
        }
    }

    /**
     * Close the underlying {@link KafkaConsumer}.
     */
    @Override
    public synchronized void close() {
        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
            Uni.createFrom().voidItem().invoke(() -> {
                LOGGER.infof("Closing consumer %s", clientId());
                if (kafkaConsumer != null) {
                    kafkaConsumer.close(kafkaApiTimeout);
                    kafkaConsumer = null;
                }
                if (consumerExecutor != null) {
                    consumerExecutor.shutdown();
                    consumerExecutor = null;
                }
                polling.compareAndSet(true, false);
            }).runSubscriptionOn(getOrCreateExecutor()).subscribeAsCompletionStage();
        }
    }

    public void terminate(KafkaConsumer<K, V> consumer, Throwable throwable) {
        this.close();
    }

    /**
     * @return the {@code client.id} property of this consumer
     */
    public String clientId() {
        return (String) props.get(ConsumerConfig.CLIENT_ID_CONFIG);
    }

    /**
     * @return the {@code group.id} property of this consumer
     */
    public String groupId() {
        return (String) props.get(ConsumerConfig.GROUP_ID_CONFIG);
    }

    /**
     * Add property to the configuration to be used when the consumer is created.
     *
     * @param key the key for property
     * @param value the value for property
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withProp(String key, String value) {
        props.put(key, value);
        return this;
    }

    /**
     * Add properties to the configuration to be used when the consumer is created.
     *
     * @param properties the properties
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withProps(Map<String, Object> properties) {
        props.putAll(properties);
        return this;
    }

    /**
     * Add configuration property for {@code client.id}
     *
     * @param clientId the client id
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withClientId(String clientId) {
        return withProp(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    }

    /**
     * Add configuration property for {@code group.id}
     *
     * @param groupId the group id
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withGroupId(String groupId) {
        return withProp(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    /**
     * Add configuration property for {@code auto.offset.reset}
     *
     * @param offsetResetStrategy the offset reset strategy
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withOffsetReset(OffsetResetStrategy offsetResetStrategy) {
        return withProp(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy.name().toLowerCase());
    }

    /**
     * Set isolation level
     *
     * @param isolationLevel the isolation level
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withIsolationLevel(IsolationLevel isolationLevel) {
        return withProp(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel.name().toLowerCase());
    }

    /**
     * Set poll timeout
     *
     * @param timeout the poll timeout
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withPollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    /**
     * Enable auto commit
     *
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withAutoCommit() {
        withProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return this;
    }

    /**
     * If set the given function is used to determine when to commit async
     *
     * @param commitPredicate the commit async predicate
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withCommitAsyncWhen(Predicate<ConsumerRecord<K, V>> commitPredicate) {
        return withCommitAsync(cr -> commitPredicate.test(cr) ? getOffsetMapFromConsumerRecord(cr) : Collections.emptyMap(),
                (offsets, exception) -> {
                });
    }

    /**
     * If set the given function is used to determine when and which offsets to commit async
     *
     * @param commitFunction the function to call for commit async
     * @param callback the callback to set for commit async
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withCommitAsync(
            Function<ConsumerRecord<K, V>, Map<TopicPartition, OffsetAndMetadata>> commitFunction,
            OffsetCommitCallback callback) {
        this.commitAsyncFunction = commitFunction;
        this.commitAsyncCallback = callback;
        return this;
    }

    /**
     * If set the given function is used to determine when to commit sync
     *
     * @param commitPredicate the commit sync predicate
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withCommitSyncWhen(Predicate<ConsumerRecord<K, V>> commitPredicate) {
        this.commitSyncFunction = cr -> commitPredicate.test(cr) ? getOffsetMapFromConsumerRecord(cr) : Collections.emptyMap();
        return this;
    }

    /**
     * If set the given function is used to determine when and which offsets to commit sync
     *
     * @param commitFunction the function to call
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withCommitSync(
            Function<ConsumerRecord<K, V>, Map<TopicPartition, OffsetAndMetadata>> commitFunction) {
        this.commitSyncFunction = commitFunction;
        return this;
    }

    /**
     * @param onPartitionsAssigned the callback to be called on partitions assigned
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withOnPartitionsAssigned(Consumer<Collection<TopicPartition>> onPartitionsAssigned) {
        this.onPartitionsAssigned = onPartitionsAssigned;
        return this;
    }

    /**
     * @param onPartitionsRevoked the callback to be called on partitions revoked
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withOnPartitionsRevoked(Consumer<Collection<TopicPartition>> onPartitionsRevoked) {
        this.onPartitionsRevoked = onPartitionsRevoked;
        return this;
    }

    /**
     * @param onTermination the callback to be called on termination
     * @return this {@link ConsumerBuilder}
     */
    public ConsumerBuilder<K, V> withOnTermination(BiConsumer<KafkaConsumer<K, V>, Throwable> onTermination) {
        this.onTermination = onTermination;
        return this;
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsetMapFromConsumerRecord(ConsumerRecord<?, ?> consumerRecord) {
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        map.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1));
        return map;
    }

    /**
     * @return the consumer group metadata
     */
    public ConsumerGroupMetadata groupMetadata() {
        return getOrCreateConsumer().groupMetadata();
    }

    /**
     * @return the current partition assignments of this consumer
     */
    public Set<TopicPartition> currentAssignment() {
        return Collections.unmodifiableSet(assignment);
    }

    private Uni<Set<TopicPartition>> assignmentUni() {
        return Uni.createFrom().item(() -> getOrCreateConsumer().assignment())
                .runSubscriptionOn(getOrCreateExecutor());
    }

    /**
     * @return the partition assignment of this consumer
     */
    public Set<TopicPartition> assignment() {
        return Uni.createFrom().item(() -> getOrCreateConsumer().assignment())
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the {@link Uni} for waiting on the assignments of partitions
     */
    public Uni<Set<TopicPartition>> waitForAssignment() {
        return waitFor(assignmentUni(), partitions -> partitions != null && !partitions.isEmpty(), pollTimeout);
    }

    /**
     * @param partition the topic partition
     * @return the position of this consumer for the given topic partition
     */
    public long position(TopicPartition partition) {
        return Uni.createFrom().item(() -> getOrCreateConsumer().position(partition))
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the position of this consumer for all assigned topic partitions
     */
    public Map<TopicPartition, OffsetAndMetadata> position() {
        return assignmentUni().onItem().transform(assignment -> {
            KafkaConsumer<K, V> consumer = getOrCreateConsumer();
            return assignment.stream().collect(Collectors.toMap(Function.identity(),
                    tp -> new OffsetAndMetadata(consumer.position(tp))));
        }).await().atMost(kafkaApiTimeout);
    }

    /**
     * Reset to the last committed position
     */
    public void resetToLastCommittedPositions() {
        Map<TopicPartition, OffsetAndMetadata> committed = committed();
        for (TopicPartition tp : assignment()) {
            OffsetAndMetadata offsetAndMetadata = committed.get(tp);
            if (offsetAndMetadata != null) {
                getOrCreateConsumer().seek(tp, offsetAndMetadata.offset());
            } else {
                getOrCreateConsumer().seekToBeginning(Collections.singleton(tp));
            }
        }
    }

    /**
     * @param partition the topic partition
     * @return the map of committed offsets for the topic partition
     */
    public OffsetAndMetadata committed(TopicPartition partition) {
        return Uni.createFrom().item(() -> getOrCreateConsumer().committed(Collections.singleton(partition)))
                .onItem().transform(map -> map.get(partition))
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param partitions the topic partitions
     * @return the map of committed offsets for topic partitions
     */
    public Map<TopicPartition, OffsetAndMetadata> committed(TopicPartition... partitions) {
        return Uni.createFrom().item(() -> getOrCreateConsumer()
                .committed(new HashSet<>(Arrays.asList(partitions))))
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @return the map of committed offsets for topic partitions assigned to this consumer
     */
    public Map<TopicPartition, OffsetAndMetadata> committed() {
        return assignmentUni().onItem()
                .transform(partitions -> getOrCreateConsumer().committed(partitions))
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * Pause the consumer
     */
    public void pause() {
        assignmentUni().onItem().invoke(partitions -> getOrCreateConsumer().pause(partitions)).replaceWithVoid()
                .await().indefinitely();
    }

    /**
     * Resume the consumer
     */
    public void resume() {
        assignmentUni().onItem().invoke(partitions -> getOrCreateConsumer().resume(partitions)).replaceWithVoid()
                .await().atMost(kafkaApiTimeout);
    }

    @SuppressWarnings({ "unchecked" })
    private Uni<ConsumerRecords<K, V>> poll() {
        return Uni.createFrom().item(() -> {
            try {
                return getOrCreateConsumer().poll(pollTimeout);
            } catch (WakeupException e) {
                return (ConsumerRecords<K, V>) ConsumerRecords.EMPTY;
            }
        }).onItem().transformToUni(cr -> {
            if (cr.isEmpty()) {
                return Uni.createFrom().item(cr).onItem().delayIt()
                        .onExecutor(getOrCreateExecutor())
                        .by(Duration.ofMillis(2));
            } else {
                return Uni.createFrom().item(cr);
            }
        });
    }

    <T> Multi<T> process(Set<String> topics, Function<Multi<ConsumerRecord<K, V>>, Multi<T>> plugFunction) {
        return Multi.createFrom().deferred(() -> {
            getOrCreateConsumer().subscribe(topics, this);
            return getConsumeMulti().plug(plugFunction);
        });
    }

    <T> Multi<T> processBatch(Set<String> topics, Function<ConsumerRecords<K, V>, Multi<T>> consumerRecordFunction) {
        return Multi.createFrom().deferred(() -> {
            getOrCreateConsumer().subscribe(topics, this);
            return getProcessingMulti(consumerRecordFunction);
        });
    }

    private <T> Multi<T> getProcessingMulti(Function<ConsumerRecords<K, V>, Multi<T>> consumerRecordFunction) {
        if (!polling.compareAndSet(false, true)) {
            return Multi.createFrom().failure(new IllegalStateException("Consumer already in use"));
        }
        return poll().repeat().indefinitely()
                .onItem().transformToMulti(consumerRecordFunction)
                .concatenate()
                .runSubscriptionOn(getOrCreateExecutor())
                .onTermination()
                .invoke((throwable, cancelled) -> this.onTermination.accept(kafkaConsumer, throwable));
    }

    private Multi<ConsumerRecord<K, V>> getConsumeMulti() {
        if (!polling.compareAndSet(false, true)) {
            return Multi.createFrom().failure(new IllegalStateException("Consumer already in use"));
        }
        return poll().repeat().indefinitely()
                .onItem().transformToMulti(cr -> Multi.createFrom().items(StreamSupport.stream(cr.spliterator(), false)))
                .concatenate()
                .runSubscriptionOn(getOrCreateExecutor())
                .plug(m -> {
                    Multi<ConsumerRecord<K, V>> multi = m;
                    if (this.commitAsyncFunction != null) {
                        multi = multi.invoke(cr -> {
                            Map<TopicPartition, OffsetAndMetadata> offsetMap = commitAsyncFunction.apply(cr);
                            if (!offsetMap.isEmpty()) {
                                getOrCreateConsumer().commitAsync(offsetMap, this.commitAsyncCallback);
                            }
                        });
                    }
                    if (this.commitSyncFunction != null) {
                        multi = multi.onItem().transformToUniAndConcatenate(cr -> {
                            Map<TopicPartition, OffsetAndMetadata> offsetMap = commitSyncFunction.apply(cr);
                            if (!offsetMap.isEmpty()) {
                                return Uni.createFrom().item(cr)
                                        .invoke(() -> getOrCreateConsumer().commitSync(offsetMap));
                            } else {
                                return Uni.createFrom().item(cr);
                            }
                        });
                    }
                    return multi.onTermination()
                            .invoke((throwable, cancelled) -> this.onTermination.accept(kafkaConsumer, throwable));
                });
    }

    /**
     * Create {@link ConsumerTask} for consuming records starting from the given offsets.
     * <p>
     * The plug function is used to modify the {@link Multi} generating the records, ex. limiting the number of records
     * produced.
     * <p>
     * The task will run until a failure occurs or is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param offsets the map of offsets to start consuming records
     * @param plugFunction the function to apply to the resulting multi
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromOffsets(Map<TopicPartition, Long> offsets,
            Function<Multi<ConsumerRecord<K, V>>, Multi<ConsumerRecord<K, V>>> plugFunction) {
        return new ConsumerTask<>(Multi.createFrom().deferred(() -> {
            getOrCreateConsumer().unsubscribe();
            Set<TopicPartition> topicPartitions = offsets.keySet();
            getOrCreateConsumer().assign(topicPartitions);
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long offset = entry.getValue();
                if (offset > -1) {
                    getOrCreateConsumer().seek(tp, offset);
                } else {
                    getOrCreateConsumer().seekToEnd(Collections.singleton(tp));
                    long lastOffset = getOrCreateConsumer().position(tp);
                    getOrCreateConsumer().seek(tp, lastOffset + offset);
                }
            }
            return getConsumeMulti().plug(plugFunction);
        }));
    }

    /**
     * Create {@link ConsumerTask} for consuming records starting from the given offsets.
     * <p>
     * The task will run until a failure occurs or is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param offsets the map of offsets to start consuming records
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromOffsets(Map<TopicPartition, Long> offsets) {
        return fromOffsets(offsets, Function.identity());
    }

    /**
     * Create {@link ConsumerTask} for consuming records starting from the given offsets.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until the {@link Multi} is terminated (with completion or failure) or
     * is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param offsets the map of offsets to start consuming records
     * @param during the duration of the producing task to run
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromOffsets(Map<TopicPartition, Long> offsets, Duration during) {
        return fromOffsets(offsets, until(during));
    }

    /**
     * Create {@link ConsumerTask} for consuming records starting from the given offsets.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run for generating the given number of records.
     *
     * @param offsets the map of offsets to start consuming records
     * @param numberOfRecords the number of records to produce
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromOffsets(Map<TopicPartition, Long> offsets, long numberOfRecords) {
        return fromOffsets(offsets, until(numberOfRecords));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     * <p>
     * The plug function is used to modify the {@link Multi} generating the records, ex. limiting the number of records
     * produced.
     * <p>
     * The task will run until a failure occurs or is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param topics the set of topics to subscribe
     * @param plugFunction the function to apply to the resulting multi
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(Set<String> topics,
            Function<Multi<ConsumerRecord<K, V>>, Multi<ConsumerRecord<K, V>>> plugFunction) {
        return new ConsumerTask<>(Multi.createFrom().deferred(() -> {
            getOrCreateConsumer().subscribe(topics, this);
            return getConsumeMulti().plug(plugFunction);
        }));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topic.
     * <p>
     * The plug function is used to modify the {@link Multi} generating the records, ex. limiting the number of records
     * produced.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until the {@link Multi} is terminated (with completion or failure) or
     * is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param topic the topic to subscribe
     * @param plugFunction the function to apply to the resulting multi
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String topic,
            Function<Multi<ConsumerRecord<K, V>>, Multi<ConsumerRecord<K, V>>> plugFunction) {
        return fromTopics(Collections.singleton(topic), plugFunction);
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until a failure occurs or is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param topics the set of topics to subscribe
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String... topics) {
        return fromTopics(new HashSet<>(Arrays.asList(topics)));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until a failure occurs or is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param topics the set of topics to subscribe
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(Set<String> topics) {
        return fromTopics(topics, Function.identity());
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until the given number of records consumed.
     *
     * @param topics the set of topics to subscribe
     * @param numberOfRecords the number of records to produce
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(Set<String> topics, long numberOfRecords) {
        return fromTopics(topics, until(numberOfRecords));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run during the given duration.
     *
     * @param topics the set of topics to subscribe
     * @param during the duration of the producing task to run
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(Set<String> topics, Duration during) {
        return fromTopics(topics, until(during));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topic.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until the given number of records consumed.
     *
     * @param topic the topic to subscribe
     * @param numberOfRecords the number of records to produce
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String topic, long numberOfRecords) {
        return fromTopics(Collections.singleton(topic), until(numberOfRecords));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topic.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run during the given duration.
     *
     * @param topic the topic to subscribe
     * @param during the duration of the producing task to run
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String topic, Duration during) {
        return fromTopics(Collections.singleton(topic), until(during));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topic.
     * <p>
     * The resulting {@link ConsumerTask} will be already started.
     * The task will run until the given number of records consumed or during the given duration.
     *
     * @param topic the topic to subscribe
     * @param numberOfRecords the number of records to produce
     * @param during the duration of the producing task to run
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String topic, long numberOfRecords, Duration during) {
        return fromTopics(Collections.singleton(topic), until(numberOfRecords, during, null));
    }

    public ConsumerTask<K, V> fromPrevious(KafkaTask<?, ?> task,
            Function<Multi<ConsumerRecord<K, V>>, Multi<ConsumerRecord<K, V>>> plugFunction) {
        Map<TopicPartition, Long> offsets = task.latestOffsets().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() + 1));
        return fromOffsets(offsets, plugFunction);
    }

    public ConsumerTask<K, V> fromPrevious(KafkaTask<?, ?> task) {
        return fromPrevious(task, Function.identity());
    }

    public ConsumerTask<K, V> fromPrevious(KafkaTask<?, ?> task, long numberOfRecords) {
        return fromPrevious(task, until(numberOfRecords));
    }

    public ConsumerTask<K, V> fromPrevious(KafkaTask<?, ?> task, Duration during) {
        return fromPrevious(task, until(during));
    }

    public ConsumerTask<K, V> fromPrevious(KafkaTask<?, ?> task, long numberOfRecords, Duration during) {
        return fromPrevious(task, until(numberOfRecords, during, null));
    }

    /**
     * Commit the given offset and close the consumer
     *
     * @param offsets the map of offsets to commit
     */
    public void commitAndClose(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Uni.createFrom().voidItem().onItem().invoke(() -> getOrCreateConsumer().commitSync(offsets))
                .onItem().invoke(this::close)
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * Commit the current offset and close the consumer
     */
    public void commitAndClose() {
        Uni.createFrom().voidItem().onItem().invoke(() -> getOrCreateConsumer().commitSync())
                .onItem().invoke(this::close)
                .runSubscriptionOn(getOrCreateExecutor())
                .await().atMost(kafkaApiTimeout);
    }

}
