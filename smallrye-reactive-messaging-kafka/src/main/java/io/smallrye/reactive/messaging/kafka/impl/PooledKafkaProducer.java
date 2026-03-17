package io.smallrye.reactive.messaging.kafka.impl;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import jakarta.enterprise.inject.Instance;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.CheckReturnValue;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;
import io.smallrye.reactive.messaging.kafka.SerializationFailureHandler;
import io.smallrye.reactive.messaging.kafka.i18n.KafkaExceptions;

public class PooledKafkaProducer<K, V> implements KafkaProducer<K, V> {

    private final CopyOnWriteArrayList<ReactiveKafkaProducer<K, V>> allProducers = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedQueue<ReactiveKafkaProducer<K, V>> availableProducers = new ConcurrentLinkedQueue<>();
    private final AtomicInteger producerIndex = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Map<String, Object> kafkaConfiguration;
    private final String channel;
    private final int closeTimeout;
    private final int maxPoolSize;
    private final ProducerInterceptor<K, V> interceptor;
    private final SerializationFailureHandler<K> keySerializationFailureHandler;
    private final SerializationFailureHandler<V> valueSerializationFailureHandler;
    private final BiConsumer<Producer<?, ?>, Map<String, Object>> onProducerCreated;
    private final Consumer<Throwable> reportFailure;

    private final String baseTransactionalId;
    private final String baseClientId;

    public PooledKafkaProducer(KafkaConnectorOutgoingConfiguration config,
            Instance<ClientCustomizer<Map<String, Object>>> configCustomizers,
            Instance<SerializationFailureHandler<?>> serializationFailureHandlers,
            Instance<ProducerInterceptor<?, ?>> producerInterceptors,
            Consumer<Throwable> reportFailure,
            BiConsumer<Producer<?, ?>, Map<String, Object>> onProducerCreated) {
        this(ReactiveKafkaProducer.getKafkaProducerConfiguration(config, configCustomizers),
                config.getChannel(), config.getCloseTimeout(),
                config.getPooledProducerInitialPoolSize(),
                config.getPooledProducerMaxPoolSize(),
                config.getLazyClient(),
                ReactiveKafkaProducer.getProducerInterceptorBean(config, producerInterceptors),
                ReactiveKafkaProducer.createSerializationFailureHandler(config.getChannel(),
                        config.getKeySerializationFailureHandler().orElse(null),
                        serializationFailureHandlers),
                ReactiveKafkaProducer.createSerializationFailureHandler(config.getChannel(),
                        config.getValueSerializationFailureHandler().orElse(null),
                        serializationFailureHandlers),
                onProducerCreated,
                reportFailure);
    }

    public PooledKafkaProducer(Map<String, Object> kafkaConfiguration,
            String channel,
            int closeTimeout,
            int initialPoolSize,
            int maxPoolSize,
            boolean lazyClient,
            ProducerInterceptor<K, V> interceptor,
            SerializationFailureHandler<K> keySerializationFailureHandler,
            SerializationFailureHandler<V> valueSerializationFailureHandler,
            BiConsumer<Producer<?, ?>, Map<String, Object>> onProducerCreated,
            Consumer<Throwable> reportFailure) {
        this.kafkaConfiguration = kafkaConfiguration;
        this.channel = channel;
        this.closeTimeout = closeTimeout;
        this.maxPoolSize = maxPoolSize;
        this.interceptor = interceptor;
        this.keySerializationFailureHandler = keySerializationFailureHandler;
        this.valueSerializationFailureHandler = valueSerializationFailureHandler;
        this.onProducerCreated = onProducerCreated;
        this.reportFailure = reportFailure;

        this.baseTransactionalId = (String) kafkaConfiguration.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        if (baseTransactionalId == null || baseTransactionalId.isEmpty()) {
            throw new IllegalArgumentException(
                    "pooled-producer requires transactional.id to be set on channel " + channel);
        }
        this.baseClientId = (String) kafkaConfiguration.get(ProducerConfig.CLIENT_ID_CONFIG);

        if (initialPoolSize < 0) {
            throw new IllegalArgumentException(
                    "pooled-producer.initial-pool-size must be >= 0 on channel " + channel);
        }
        if (maxPoolSize < 1) {
            throw new IllegalArgumentException(
                    "pooled-producer.max-pool-size must be >= 1 on channel " + channel);
        }
        if (initialPoolSize > maxPoolSize) {
            throw new IllegalArgumentException(
                    "pooled-producer.initial-pool-size (" + initialPoolSize
                            + ") must be <= pooled-producer.max-pool-size (" + maxPoolSize
                            + ") on channel " + channel);
        }

        // Pre-create initial producers using the outer lazy-client setting.
        // With lazy-client=false (default), these eagerly initialize the Kafka producer
        // and call initTransactions(). On-demand producers continue to use lazyClient=true.
        for (int i = 0; i < initialPoolSize; i++) {
            ReactiveKafkaProducer<K, V> producer = createProducer(producerIndex.incrementAndGet(), lazyClient);
            availableProducers.offer(producer);
        }
    }

    private ReactiveKafkaProducer<K, V> acquireProducer() {
        if (closed.get()) {
            throw KafkaExceptions.ex.pooledProducerClosed();
        }
        ReactiveKafkaProducer<K, V> p = availableProducers.poll();
        if (p != null) {
            return p;
        }
        // Try to create a new producer if under max
        int index = producerIndex.incrementAndGet();
        if (index > maxPoolSize) {
            // Counter exceeded max — but a producer might have been released in the meantime
            ReactiveKafkaProducer<K, V> r = availableProducers.poll();
            if (r != null) {
                return r;
            }
            throw KafkaExceptions.ex.pooledProducerPoolExhausted(maxPoolSize, channel);
        }
        return createProducer(index);
    }

    private void releaseProducer(ReactiveKafkaProducer<K, V> p) {
        availableProducers.offer(p);
    }

    private ReactiveKafkaProducer<K, V> createProducer(int index) {
        return createProducer(index, true);
    }

    private ReactiveKafkaProducer<K, V> createProducer(int index, boolean lazyClient) {
        String suffix = "-" + index;
        String transactionalId = baseTransactionalId + suffix;
        String clientId = baseClientId + suffix;

        Map<String, Object> producerConfig = new HashMap<>(kafkaConfiguration);
        producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        // On-demand producers use lazyClient=true: they are created on-demand,
        // potentially on the Vert.x event loop thread where blocking is not allowed.
        // The actual Kafka producer and initTransactions() will run on the kafka sending thread
        // when the first operation (e.g. beginTransaction()) is performed.
        // Pre-created producers use the outer lazy-client setting.
        ReactiveKafkaProducer<K, V> producer = new ReactiveKafkaProducer<>(producerConfig, channel, closeTimeout,
                lazyClient, interceptor, keySerializationFailureHandler, valueSerializationFailureHandler,
                reportFailure, onProducerCreated);

        allProducers.add(producer);
        log.pooledProducerCreated(index, channel, transactionalId);
        return producer;
    }

    @Override
    @CheckReturnValue
    public Uni<RecordMetadata> send(ProducerRecord<K, V> record) {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("send");
    }

    @Override
    @CheckReturnValue
    public Uni<Void> flush() {
        if (allProducers.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return Uni.join().all(
                allProducers.stream()
                        .map(ReactiveKafkaProducer::flush)
                        .toList())
                .andCollectFailures()
                .replaceWithVoid();
    }

    @Override
    @CheckReturnValue
    public Uni<List<PartitionInfo>> partitionsFor(String topic) {
        ReactiveKafkaProducer<K, V> anyProducer = allProducers.stream().findFirst().orElse(null);
        if (anyProducer != null) {
            return anyProducer.partitionsFor(topic);
        }
        // No producers available yet — create one via the pool
        ReactiveKafkaProducer<K, V> producer = acquireProducer();
        return producer.partitionsFor(topic)
                .eventually(() -> releaseProducer(producer));
    }

    @Override
    @CheckReturnValue
    public Uni<Void> initTransactions() {
        // Inner producers handle initTransactions lazily on first use
        return Uni.createFrom().voidItem();
    }

    @Override
    @CheckReturnValue
    public Uni<Void> beginTransaction() {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("beginTransaction");
    }

    @Override
    @CheckReturnValue
    public Uni<Void> commitTransaction() {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("commitTransaction");
    }

    @Override
    @CheckReturnValue
    public Uni<Void> abortTransaction() {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("abortTransaction");
    }

    @Override
    @CheckReturnValue
    public Uni<Void> sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
            ConsumerGroupMetadata groupMetadata) {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("sendOffsetsToTransaction");
    }

    @Override
    public KafkaProducer<K, V> transactionScope() {
        return new TransactionScope();
    }

    @Override
    public Map<String, ?> configuration() {
        return kafkaConfiguration;
    }

    @Override
    @CheckReturnValue
    public <R> Uni<R> runOnSendingThread(Function<Producer<K, V>, R> action) {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("runOnSendingThread");
    }

    @Override
    @CheckReturnValue
    public Uni<Void> runOnSendingThread(java.util.function.Consumer<Producer<K, V>> action) {
        throw KafkaExceptions.ex.pooledProducerUnsupportedOperation("runOnSendingThread");
    }

    @Override
    public Producer<K, V> unwrap() {
        return allProducers.stream()
                .findFirst()
                .map(ReactiveKafkaProducer::unwrap)
                .orElse(null);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.closingPooledProducers(allProducers.size(), channel);
            for (ReactiveKafkaProducer<K, V> producer : allProducers) {
                try {
                    producer.close();
                } catch (Exception e) {
                    log.errorWhileClosingWriteStream(e);
                }
            }
            allProducers.clear();
            availableProducers.clear();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public boolean isPooled() {
        return true;
    }

    /**
     * @return the list of all producers in the pool. Visible for testing.
     */
    public List<ReactiveKafkaProducer<K, V>> getProducers() {
        return allProducers;
    }

    /**
     * A transaction scope that acquires a single producer from the pool for the duration
     * of a transaction. All sends within this scope go through that one producer,
     * ensuring a single atomic Kafka transaction.
     * <p>
     * Multiple scopes can run concurrently, each using a different pooled producer.
     */
    private class TransactionScope implements KafkaProducer<K, V> {

        private final AtomicReference<ReactiveKafkaProducer<K, V>> producer = new AtomicReference<>();

        @Override
        @CheckReturnValue
        public Uni<Void> beginTransaction() {
            return acquire().beginTransaction()
                    .onFailure().invoke(() -> {
                        ReactiveKafkaProducer<K, V> released = producer.getAndSet(null);
                        if (released != null) {
                            releaseProducer(released);
                        }
                    });
        }

        synchronized ReactiveKafkaProducer<K, V> acquire() {
            ReactiveKafkaProducer<K, V> p = producer.get();
            if (p == null) {
                p = acquireProducer();
                producer.set(p);
            }
            return p;
        }

        @Override
        @CheckReturnValue
        public Uni<RecordMetadata> send(ProducerRecord<K, V> record) {
            ReactiveKafkaProducer<K, V> p = producer.get();
            if (p != null) {
                return p.send(record);
            }
            throw KafkaExceptions.ex.transactionNotStarted();
        }

        @Override
        @CheckReturnValue
        public Uni<Void> flush() {
            ReactiveKafkaProducer<K, V> p = producer.get();
            if (p == null) {
                return Uni.createFrom().voidItem();
            }
            return p.flush();
        }

        @Override
        @CheckReturnValue
        public Uni<Void> commitTransaction() {
            ReactiveKafkaProducer<K, V> p = producer.getAndSet(null);
            if (p == null) {
                return Uni.createFrom().voidItem();
            }
            try {
                return p.commitTransaction().eventually(() -> releaseProducer(p));
            } catch (Exception e) {
                releaseProducer(p);
                throw e;
            }
        }

        @Override
        @CheckReturnValue
        public Uni<Void> abortTransaction() {
            ReactiveKafkaProducer<K, V> p = producer.getAndSet(null);
            if (p == null) {
                return Uni.createFrom().voidItem();
            }
            try {
                return p.abortTransaction().eventually(() -> releaseProducer(p));
            } catch (Exception e) {
                releaseProducer(p);
                throw e;
            }
        }

        @Override
        @CheckReturnValue
        public Uni<Void> sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                ConsumerGroupMetadata groupMetadata) {
            ReactiveKafkaProducer<K, V> p = producer.get();
            if (p == null) {
                return Uni.createFrom().voidItem();
            }
            return p.sendOffsetsToTransaction(offsets, groupMetadata);
        }

        // Delegate non-transaction methods to the outer producer

        @Override
        public Map<String, ?> configuration() {
            return PooledKafkaProducer.this.configuration();
        }

        @Override
        @CheckReturnValue
        public Uni<Void> initTransactions() {
            return PooledKafkaProducer.this.initTransactions();
        }

        @Override
        @CheckReturnValue
        public Uni<List<PartitionInfo>> partitionsFor(String topic) {
            return PooledKafkaProducer.this.partitionsFor(topic);
        }

        @Override
        @CheckReturnValue
        public <R> Uni<R> runOnSendingThread(Function<Producer<K, V>, R> action) {
            return PooledKafkaProducer.this.runOnSendingThread(action);
        }

        @Override
        @CheckReturnValue
        public Uni<Void> runOnSendingThread(Consumer<Producer<K, V>> action) {
            return PooledKafkaProducer.this.runOnSendingThread(action);
        }

        @Override
        public Producer<K, V> unwrap() {
            return acquire().unwrap();
        }

        @Override
        public void close() {
            // TransactionScope does not own the producers — closing is handled by the outer producer
        }

        @Override
        public boolean isClosed() {
            return PooledKafkaProducer.this.isClosed();
        }

        @Override
        public boolean isPooled() {
            return true;
        }

    }
}
