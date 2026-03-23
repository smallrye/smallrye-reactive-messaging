package io.smallrye.reactive.messaging.kafka.companion;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.internals.ShareAcquireMode;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Kafka share consumer wrapper for creating tasks ({@link ConsumerTask}) that consume records using share groups.
 * <p>
 * Share groups provide cooperative consumption without explicit partition assignment.
 * Records are acknowledged individually using {@link AcknowledgeType}.
 *
 * @param <K> The record key type
 * @param <V> The record value type
 */
public class ShareConsumerBuilder<K, V> implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(ShareConsumerBuilder.class);

    private final Map<String, Object> props;
    private final Function<Map<String, Object>, KafkaShareConsumer<K, V>> consumerCreator;

    private KafkaShareConsumer<K, V> kafkaShareConsumer;
    private ScheduledExecutorService consumerExecutor;

    private BiConsumer<KafkaShareConsumer<K, V>, Throwable> onTermination = this::terminate;

    private final Duration kafkaApiTimeout;
    private Duration pollTimeout = Duration.ofMillis(100);
    private final AtomicBoolean polling = new AtomicBoolean(false);

    private Function<ConsumerRecord<K, V>, AcknowledgeType> acknowledgeFunction;

    /**
     * Creates a new {@link ShareConsumerBuilder} with deserializer instances.
     *
     * @param props the initial properties for consumer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keyDeser the deserializer for record keys
     * @param valueDeser the deserializer for record values
     */
    public ShareConsumerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            Deserializer<K> keyDeser, Deserializer<V> valueDeser) {
        this.props = props;
        this.kafkaApiTimeout = kafkaApiTimeout;
        this.consumerCreator = p -> new KafkaShareConsumer<>(p, keyDeser, valueDeser);
    }

    /**
     * Creates a new {@link ShareConsumerBuilder} with deserializer class names.
     *
     * @param props the initial properties for consumer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keyDeserializerType the deserializer class name for record keys
     * @param valueDeserializerType the deserializer class name for record values
     */
    public ShareConsumerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            String keyDeserializerType, String valueDeserializerType) {
        this.props = props;
        this.kafkaApiTimeout = kafkaApiTimeout;
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerType);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerType);
        this.consumerCreator = KafkaShareConsumer::new;
    }

    private synchronized KafkaShareConsumer<K, V> getOrCreateConsumer() {
        if (this.kafkaShareConsumer == null) {
            this.kafkaShareConsumer = consumerCreator.apply(props);
        }
        return this.kafkaShareConsumer;
    }

    private synchronized ScheduledExecutorService getOrCreateExecutor() {
        if (this.consumerExecutor == null) {
            this.consumerExecutor = Executors
                    .newSingleThreadScheduledExecutor(c -> new Thread(c, "share-consumer-" + clientId()));
        }
        return this.consumerExecutor;
    }

    /**
     * @return the underlying {@link KafkaShareConsumer}, may be {@code null} if consumer is not yet created.
     */
    public KafkaShareConsumer<K, V> unwrap() {
        return kafkaShareConsumer;
    }

    @Override
    public synchronized void close() {
        if (kafkaShareConsumer != null) {
            kafkaShareConsumer.wakeup();
            Uni.createFrom().voidItem().invoke(() -> {
                LOGGER.infof("Closing share consumer %s", clientId());
                if (kafkaShareConsumer != null) {
                    kafkaShareConsumer.close(kafkaApiTimeout);
                    kafkaShareConsumer = null;
                }
                if (consumerExecutor != null) {
                    consumerExecutor.shutdown();
                    consumerExecutor = null;
                }
                polling.compareAndSet(true, false);
            }).runSubscriptionOn(getOrCreateExecutor()).subscribeAsCompletionStage();
        }
    }

    private void terminate(KafkaShareConsumer<K, V> consumer, Throwable throwable) {
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
     * Add property to the configuration.
     *
     * @param key the key for property
     * @param value the value for property
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withProp(String key, Object value) {
        props.put(key, value);
        return this;
    }

    /**
     * Add properties to the configuration.
     *
     * @param properties the properties
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withProps(Map<String, Object> properties) {
        props.putAll(properties);
        return this;
    }

    /**
     * Set the client id.
     *
     * @param clientId the client id
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withClientId(String clientId) {
        return withProp(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    }

    /**
     * Set the group id.
     *
     * @param groupId the group id
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withGroupId(String groupId) {
        return withProp(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    /**
     * Add configuration property for {@code share.auto.offset.reset}
     *
     * @param offsetResetStrategy the offset reset strategy, e.g. {@code "earliest"}, {@code "latest"},
     *        {@code "by_duration:PT1H"}
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withOffsetReset(String offsetResetStrategy) {
        return withProp("share.auto.offset.reset", offsetResetStrategy);
    }

    /**
     * Acknowledge explicitly records matching the given function with returned acknowledge type.
     *
     * @param function the function to determine the acknowledge type for each record
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withExplicitAck(Function<ConsumerRecord<K, V>, AcknowledgeType> function) {
        this.acknowledgeFunction = function;
        return withProp(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
    }

    /**
     * Set the share acquire mode controlling fetch behavior (KIP-1206).
     * <ul>
     * <li>{@link ShareAcquireMode#BATCH_OPTIMIZED} - Soft limit, may exceed {@code max.poll.records} to align with batch
     * boundaries (default).</li>
     * <li>{@link ShareAcquireMode#RECORD_LIMIT} - Strict enforcement, acquires records only up to
     * {@code max.poll.records}.</li>
     * </ul>
     *
     * @param mode the share acquire mode
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withAcquireMode(ShareAcquireMode mode) {
        return withProp("share.acquire.mode", mode.toString());
    }

    /**
     * Set poll timeout.
     *
     * @param timeout the poll timeout
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withPollTimeout(Duration timeout) {
        this.pollTimeout = timeout;
        return this;
    }

    /**
     * Set the termination callback.
     *
     * @param onTermination the callback to be called on termination
     * @return this {@link ShareConsumerBuilder}
     */
    public ShareConsumerBuilder<K, V> withOnTermination(BiConsumer<KafkaShareConsumer<K, V>, Throwable> onTermination) {
        this.onTermination = onTermination;
        return this;
    }

    @SuppressWarnings("unchecked")
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
        }).onFailure(IllegalStateException.class)
                .retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .indefinitely();
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     *
     * @param topics the set of topics to subscribe
     * @param plugFunction the function to apply to the resulting multi
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(Set<String> topics,
            Function<Multi<ConsumerRecord<K, V>>, Multi<ConsumerRecord<K, V>>> plugFunction) {
        return new ConsumerTask<>(Multi.createFrom().deferred(() -> {
            if (!polling.compareAndSet(false, true)) {
                return Multi.createFrom().failure(new IllegalStateException("Consumer already in use"));
            }
            getOrCreateConsumer().subscribe(topics);
            return getConsumeMulti().plug(plugFunction);
        }));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     *
     * @param topics the set of topics to subscribe
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(Set<String> topics) {
        return fromTopics(topics, Function.identity());
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topics.
     *
     * @param topics the topics to subscribe
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String... topics) {
        return fromTopics(new HashSet<>(Arrays.asList(topics)));
    }

    /**
     * Create {@link ConsumerTask} for consuming the given number of records from the given topic.
     *
     * @param topic the topic to subscribe
     * @param numberOfRecords the number of records to consume
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String topic, long numberOfRecords) {
        return fromTopics(Collections.singleton(topic), RecordQualifiers.until(numberOfRecords));
    }

    /**
     * Create {@link ConsumerTask} for consuming records from the given topic during the given duration.
     *
     * @param topic the topic to subscribe
     * @param during the duration of the consuming task to run
     * @return the {@link ConsumerTask}
     */
    public ConsumerTask<K, V> fromTopics(String topic, Duration during) {
        return fromTopics(Collections.singleton(topic), RecordQualifiers.until(during));
    }

    private Multi<ConsumerRecord<K, V>> getConsumeMulti() {
        return poll().repeat().indefinitely()
                .onItem()
                .transformToMulti(cr -> Multi.createFrom().items(StreamSupport.stream(cr.spliterator(), false)))
                .concatenate()
                .runSubscriptionOn(getOrCreateExecutor())
                .plug(m -> {
                    Multi<ConsumerRecord<K, V>> multi = m;
                    if (this.acknowledgeFunction != null) {
                        multi = multi.invoke(cr -> {
                            AcknowledgeType ackType = acknowledgeFunction.apply(cr);
                            if (ackType != null) {
                                getOrCreateConsumer().acknowledge(cr, ackType);
                            }
                        });
                    }
                    return multi.onTermination()
                            .invoke((throwable, cancelled) -> this.onTermination.accept(kafkaShareConsumer, throwable));
                });
    }
}
