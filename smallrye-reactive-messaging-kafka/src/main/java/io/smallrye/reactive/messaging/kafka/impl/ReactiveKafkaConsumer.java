package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions.ex;
import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.i18n.ProviderLogging;
import io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.fault.DeserializerWrapper;
import io.vertx.core.Context;

public class ReactiveKafkaConsumer<K, V> implements io.smallrye.reactive.messaging.kafka.KafkaConsumer<K, V> {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Avoid concurrent call to `poll`
     */
    private final AtomicBoolean polling = new AtomicBoolean(false);
    private final KafkaSource<K, V> source;

    private Consumer<K, V> consumer;
    private final KafkaConnectorIncomingConfiguration configuration;
    private final Duration pollTimeout;
    private ConsumerRebalanceListener rebalanceListener;

    private final AtomicBoolean paused = new AtomicBoolean();

    private final ScheduledExecutorService kafkaWorker;
    private final KafkaRecordStream<K, V> stream;
    private final KafkaRecordBatchStream<K, V> batchStream;
    private final Map<String, Object> kafkaConfiguration;

    public ReactiveKafkaConsumer(KafkaConnectorIncomingConfiguration config,
            KafkaSource<K, V> source) {
        this.configuration = config;
        this.source = source;
        kafkaConfiguration = getKafkaConsumerConfiguration(configuration, source.getConsumerGroup(),
                source.getConsumerIndex());

        Instance<DeserializationFailureHandler<?>> failureHandlers = source.getDeserializationFailureHandlers();
        String keyDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valueDeserializerCN = (String) kafkaConfiguration.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        if (valueDeserializerCN == null) {
            throw ex.missingValueDeserializer(config.getChannel(), config.getChannel());
        }

        Deserializer<K> keyDeserializer = new DeserializerWrapper<>(keyDeserializerCN, true,
                getDeserializationHandler(true, failureHandlers), source, config.getFailOnDeserializationFailure());
        Deserializer<V> valueDeserializer = new DeserializerWrapper<>(valueDeserializerCN, false,
                getDeserializationHandler(false, failureHandlers), source, config.getFailOnDeserializationFailure());

        // Configure the underlying deserializers
        keyDeserializer.configure(kafkaConfiguration, true);
        valueDeserializer.configure(kafkaConfiguration, false);

        pollTimeout = Duration.ofMillis(config.getPollTimeout());

        kafkaWorker = Executors.newSingleThreadScheduledExecutor(KafkaPollingThread::new);
        consumer = new KafkaConsumer<>(kafkaConfiguration, keyDeserializer, valueDeserializer);
        stream = new KafkaRecordStream<>(this, config, source.getContext().getDelegate());
        batchStream = new KafkaRecordBatchStream<>(this, config, source.getContext().getDelegate());
    }

    public void setRebalanceListener() {
        try {
            rebalanceListener = RebalanceListeners.createRebalanceListener(this, configuration, source.getConsumerGroup(),
                    source.getConsumerRebalanceListeners(), source.getCommitHandler());
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    // Visible to use for rebalance on MockConsumer which doesn't call listeners
    public ConsumerRebalanceListener getRebalanceListener() {
        return this.rebalanceListener;
    }

    @Override
    public <T> Uni<T> runOnPollingThread(Function<Consumer<K, V>, T> action) {
        return Uni.createFrom().item(() -> action.apply(consumer))
                .runSubscriptionOn(kafkaWorker);
    }

    @Override
    public Uni<Void> runOnPollingThread(java.util.function.Consumer<Consumer<K, V>> action) {
        return Uni.createFrom().<Void> item(() -> {
            action.accept(consumer);
            return null;
        })
                .runSubscriptionOn(kafkaWorker);
    }

    Uni<Void> executeWithDelay(Runnable action, Duration delay) {
        return Uni.createFrom().emitter(e -> {
            kafkaWorker.schedule(() -> {
                try {
                    action.run();
                } catch (Exception ex) {
                    e.fail(ex);
                    return;
                }
                e.complete(null);
            }, delay.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    @SuppressWarnings("unchecked")
    Uni<ConsumerRecords<K, V>> poll() {
        if (polling.compareAndSet(false, true)) {
            return runOnPollingThread(c -> paused.get() ? c.poll(Duration.ZERO) : c.poll(pollTimeout))
                    .eventually(() -> polling.set(false))
                    .onFailure(WakeupException.class).recoverWithItem((ConsumerRecords<K, V>) ConsumerRecords.EMPTY);
        } else {
            // polling already in progress
            return Uni.createFrom().item((ConsumerRecords<K, V>) ConsumerRecords.EMPTY);
        }
    }

    @Override
    public Uni<Set<TopicPartition>> pause() {
        if (paused.compareAndSet(false, true)) {
            return runOnPollingThread(c -> {
                Set<TopicPartition> tps = consumer.assignment();
                consumer.pause(tps);
                return tps;
            });
        } else {
            return runOnPollingThread((Function<Consumer<K, V>, Set<TopicPartition>>) Consumer::paused);
        }
    }

    @Override
    public Uni<Set<TopicPartition>> paused() {
        return runOnPollingThread((Function<Consumer<K, V>, Set<TopicPartition>>) Consumer::paused);
    }

    @Override
    public Uni<Map<TopicPartition, OffsetAndMetadata>> committed(TopicPartition... tps) {
        return runOnPollingThread(c -> {
            return c.committed(new LinkedHashSet<>(Arrays.asList(tps)));
        });
    }

    public Multi<ConsumerRecord<K, V>> subscribe(Set<String> topics) {
        return runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        })
                .onItem().transformToMulti(v -> stream);
    }

    public Multi<ConsumerRecord<K, V>> subscribe(Pattern topics) {
        return runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        })
                .onItem().transformToMulti(v -> stream);
    }

    Multi<ConsumerRecords<K, V>> subscribeBatch(Set<String> topics) {
        return runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        })
                .onItem().transformToMulti(v -> batchStream);
    }

    Multi<ConsumerRecords<K, V>> subscribeBatch(Pattern topics) {
        return runOnPollingThread(c -> {
            c.subscribe(topics, rebalanceListener);
        })
                .onItem().transformToMulti(v -> batchStream);
    }

    @Override
    public Uni<Void> resume() {
        if (paused.get()) {
            return runOnPollingThread(c -> {
                Set<TopicPartition> assignment = c.assignment();
                consumer.resume(assignment);
            }).invoke(() -> paused.set(false));
        } else {
            return Uni.createFrom().voidItem();
        }
    }

    private Map<String, Object> getKafkaConsumerConfiguration(KafkaConnectorIncomingConfiguration configuration,
            String consumerGroup, int index) {
        Map<String, Object> map = new HashMap<>();
        JsonHelper.asJsonObject(configuration.config())
                .forEach(e -> map.put(e.getKey(), e.getValue().toString()));
        map.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);

        if (!map.containsKey(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG)) {
            // If no backoff is set, use 10s, it avoids high load on disconnection.
            map.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        }

        String servers = configuration.getBootstrapServers();
        if (!map.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.configServers(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        }

        if (!map.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            log.keyDeserializerOmitted();
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, configuration.getKeyDeserializer());
        }

        if (!map.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            log.disableAutoCommit(configuration.getChannel());
            map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }

        // Consumer id generation:
        // 1. If we don't have an index and no client id set in the config, add one
        // 2. If we don't have an index and a client id set in the config, use it
        // 3. If we have an index and no client id set in the config, add one suffixed with the index
        // 4. If we have an index and a client id set in the config, suffix the index

        if (index == -1) {
            map.computeIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, k -> {
                // Case 1
                return "kafka-consumer-" + configuration.getChannel();
            });
            // Case 2 - nothing to do
        } else {
            String configuredClientId = (String) map.get(ConsumerConfig.CLIENT_ID_CONFIG);
            if (configuredClientId == null) {
                // Case 3
                configuredClientId = "kafka-consumer-" + configuration.getChannel() + "-" + index;
            } else {
                // Case 4
                configuredClientId = configuredClientId + "-" + index;
            }
            map.put(ConsumerConfig.CLIENT_ID_CONFIG, configuredClientId);
        }

        ConfigurationCleaner.cleanupConsumerConfiguration(map);

        return map;
    }

    private <T> DeserializationFailureHandler<T> getDeserializationHandler(boolean isKey,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers) {
        return createDeserializationFailureHandler(isKey, deserializationFailureHandlers, configuration);

    }

    @SuppressWarnings({ "unchecked" })
    private static <T> DeserializationFailureHandler<T> createDeserializationFailureHandler(boolean isKey,
            Instance<DeserializationFailureHandler<?>> deserializationFailureHandlers,
            KafkaConnectorIncomingConfiguration configuration) {
        String name = isKey ? configuration.getKeyDeserializationFailureHandler().orElse(null)
                : configuration.getValueDeserializationFailureHandler().orElse(null);

        if (name == null) {
            return null;
        }

        Instance<DeserializationFailureHandler<?>> matching = deserializationFailureHandlers
                .select(Identifier.Literal.of(name));
        if (matching.isUnsatisfied()) {
            // this `if` block should be removed when support for the `@Named` annotation is removed
            matching = deserializationFailureHandlers.select(NamedLiteral.of(name));
            if (!matching.isUnsatisfied()) {
                ProviderLogging.log.deprecatedNamed();
            }
        }

        if (matching.isUnsatisfied()) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel());
        } else if (matching.stream().count() > 1) {
            throw ex.unableToFindDeserializationFailureHandler(name, configuration.getChannel(),
                    (int) matching.stream().count());
        } else if (matching.stream().count() == 1) {
            return (DeserializationFailureHandler<T>) matching.get();
        } else {
            return null;
        }
    }

    public String get(String attribute) {
        return (String) kafkaConfiguration.get(attribute);
    }

    @Override
    public Consumer<K, V> unwrap() {
        return consumer;
    }

    @Override
    public Uni<Void> commit(
            Map<TopicPartition, OffsetAndMetadata> map) {
        return runOnPollingThread(c -> {
            c.commitSync(map);
        });
    }

    public Map<String, ?> configuration() {
        return kafkaConfiguration;
    }

    public void close() {
        int timeout = configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class).orElse(1000);
        if (closed.compareAndSet(false, true)) {
            Uni<Void> uni = runOnPollingThread(c -> {
                c.close(Duration.ofMillis(timeout));
            })
                    .onItem().invoke(kafkaWorker::shutdown);
            // Interrupt polling
            consumer.wakeup();
            if (Context.isOnEventLoopThread()) {
                // We can't block, just forget the result
                uni.subscribeAsCompletionStage();
            } else {
                uni.await().atMost(Duration.ofMillis(timeout * 2L));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void injectClient(MockConsumer<?, ?> consumer) {
        Consumer<K, V> cons = this.consumer;
        this.consumer = (Consumer<K, V>) consumer;
        cons.close();
    }

    @Override
    public Uni<Map<TopicPartition, Long>> getPositions() {
        return runOnPollingThread(c -> {
            Map<TopicPartition, Long> map = new HashMap<>();
            c.assignment()
                    .forEach(tp -> map.put(tp, c.position(tp)));
            return map;
        });
    }

    @Override
    public Uni<Set<TopicPartition>> getAssignments() {
        return runOnPollingThread((Function<Consumer<K, V>, Set<TopicPartition>>) Consumer::assignment);
    }

    @Override
    public Uni<Void> seek(TopicPartition partition, long offset) {
        return runOnPollingThread(c -> {
            c.seek(partition, offset);
        });
    }

    @Override
    public Uni<Void> seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        return runOnPollingThread(c -> {
            c.seek(partition, offsetAndMetadata);
        });
    }

    @Override
    public Uni<Void> seekToBeginning(Collection<TopicPartition> partitions) {
        return runOnPollingThread(c -> {
            c.seekToBeginning(partitions);
        });
    }

    @Override
    public Uni<Void> seekToEnd(Collection<TopicPartition> partitions) {
        return runOnPollingThread(c -> {
            c.seekToEnd(partitions);
        });
    }

    boolean isClosed() {
        return closed.get();
    }

    boolean isPaused() {
        return paused.get();
    }

    void removeFromQueueRecordsFromTopicPartitions(Collection<TopicPartition> partitions) {
        this.stream.removeFromQueueRecordsFromTopicPartitions(partitions);
    }
}
