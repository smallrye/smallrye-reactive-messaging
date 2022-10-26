package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.until;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.jboss.logging.Logger;

import com.opencsv.CSVReader;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.MultiFlatten;
import io.smallrye.mutiny.unchecked.Unchecked;

/**
 * Kafka producer wrapper for creating tasks ({@link ProducerTask}) that produce records.
 * <p>
 * The wrapped Kafka producer is created lazily when a {@link ProducerTask} is created and started.
 * Until the producer is created builder methods prefixed with {@code with-} act to provide additional configuration.
 * <p>
 * The created producer is closed automatically when the {@code task} is terminated.
 * The producer uses an underlying {@code ExecutorService} to send records.
 *
 * @param <K> The record key type
 * @param <V> The record value type
 */
public class ProducerBuilder<K, V> implements Closeable {

    private final static Logger LOGGER = Logger.getLogger(ProducerBuilder.class);

    /**
     * Map of properties to use for creating Kafka producer
     */
    private final Map<String, Object> props;

    /**
     * Function to create the {@link KafkaProducer}
     */
    private final Function<Map<String, Object>, KafkaProducer<K, V>> producerCreator;

    /**
     * Duration for default api timeout
     */
    private final Duration kafkaApiTimeout;

    /**
     * Serde for key if provided in constructor
     * <p>
     * May be {@code null}
     */
    private Serde<K> keySerde;

    /**
     * Serde for value if provided in constructor
     * <p>
     * May be {@code null}
     */
    private Serde<V> valueSerde;

    /**
     * Kafka producer
     */
    private KafkaProducer<K, V> kafkaProducer;

    /**
     * Executor service to use sending records to Kafka
     */
    private ExecutorService executorService;

    /**
     * Callback to invoke on producer termination
     */
    private BiConsumer<KafkaProducer<K, V>, Throwable> onTermination = this::terminate;

    /**
     * Concurrency level for producer writes, default is 1. Without concurrency records are sent one after the other.
     */
    private int concurrency = 1;

    /**
     * Creates a new {@link ProducerBuilder}.
     * <p>
     * On producer creation, key and value serializers will be created and {@link Serializer#configure} methods will be called.
     *
     * @param props the initial properties for producer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keySerializerClassName the serializer class name for record keys
     * @param valueSerializerClassName the serializer class name for record values
     */
    public ProducerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            String keySerializerClassName,
            String valueSerializerClassName) {
        this.props = props;
        this.kafkaApiTimeout = kafkaApiTimeout;
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
        this.producerCreator = KafkaProducer::new;
    }

    /**
     * Creates a new {@link ProducerBuilder}.
     * <p>
     * Note that on producer creation {@link Serializer#configure} methods will NOT be called.
     *
     * @param props the initial properties for producer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keySerializer the serializer for record keys
     * @param valueSerializer the serializer for record values
     */
    public ProducerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.props = props;
        this.kafkaApiTimeout = kafkaApiTimeout;
        this.producerCreator = p -> new KafkaProducer<>(p, keySerializer, valueSerializer);
    }

    /**
     * Creates a new {@link ProducerBuilder}.
     * <p>
     * Note that on producer creation {@link Serializer#configure} methods will NOT be called.
     *
     * @param props the initial properties for producer creation
     * @param kafkaApiTimeout the timeout for api calls to Kafka
     * @param keySerde the Serde for record keys
     * @param valueSerde the Serde for record values
     */
    public ProducerBuilder(Map<String, Object> props, Duration kafkaApiTimeout,
            Serde<K> keySerde, Serde<V> valueSerde) {
        this(props, kafkaApiTimeout, keySerde.serializer(), valueSerde.serializer());
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    private synchronized KafkaProducer<K, V> getOrCreateProducer() {
        if (kafkaProducer == null) {
            this.kafkaProducer = producerCreator.apply(props);
            if (props.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
                kafkaProducer.initTransactions();
            }
        }
        return kafkaProducer;
    }

    private synchronized ExecutorService getOrCreateExecutor() {
        if (executorService == null) {
            executorService = Executors.newFixedThreadPool(1, c -> new Thread(c, "producer-" + clientId()));
        }
        return executorService;
    }

    /**
     * @return the underlying {@link KafkaProducer}, may be {@code null} if producer is not yet created.
     */
    public KafkaProducer<K, V> unwrap() {
        return kafkaProducer;
    }

    /**
     * Close the underlying {@link KafkaProducer} and {@link ExecutorService}.
     */
    @Override
    public synchronized void close() {
        if (kafkaProducer != null) {
            LOGGER.infof("Closing producer %s", clientId());
            // Kafka producer is thread-safe, we can call close on the caller thread
            kafkaProducer.flush();
            kafkaProducer.close(kafkaApiTimeout);
            kafkaProducer = null;
            executorService.shutdown();
            executorService = null;
        }
    }

    private void terminate(KafkaProducer<K, V> producer, Throwable throwable) {
        if (isTransactional()) {
            if (throwable == null) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }
        this.close();
    }

    /**
     * Add property to the configuration to be used when the producer is created.
     *
     * @param key the key for property
     * @param value the value for property
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withProp(String key, String value) {
        props.put(key, value);
        return this;
    }

    /**
     * Add properties to the configuration to be used when the producer is created.
     *
     * @param properties the properties
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withProps(Map<String, String> properties) {
        props.putAll(properties);
        return this;
    }

    /**
     * Add configuration property for {@code client.id} for this producer to be used when the producer is created.
     *
     * @param clientId the client id
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withClientId(String clientId) {
        return withProp(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    }

    /**
     * Add configuration property for {@code transactional.id} for this producer to be used when the producer is created.
     *
     * @param transactionalId the transactional id
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withTransactionalId(String transactionalId) {
        return withProp(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }

    /**
     * Add configuration property for {@code transactional.id} for this producer to be used when the producer is created.
     *
     * @param onTermination the transaction consumer group
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withOnTermination(BiConsumer<KafkaProducer<K, V>, Throwable> onTermination) {
        Objects.requireNonNull(props.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG), "transactional id");
        this.onTermination = onTermination;
        return this;
    }

    /**
     * Set the concurrency level for producer writes.
     *
     * @param concurrency the concurrency for producer writes
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withConcurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    /**
     * Set the concurrency level for producer writes to 1024.
     *
     * @return this {@link ProducerBuilder}
     */
    public ProducerBuilder<K, V> withConcurrency() {
        return withConcurrency(1024);
    }

    /**
     * @return the client id
     */
    public String clientId() {
        return (String) props.get(ProducerConfig.CLIENT_ID_CONFIG);
    }

    /**
     * @return {@code true} if the producer configuration contains {@code transactional.id}, else returns {@code false}.
     */
    public boolean isTransactional() {
        return props.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    }

    private Uni<RecordMetadata> record(ProducerRecord<K, V> record) {
        return Uni.createFrom().<RecordMetadata> emitter(em -> getOrCreateProducer().send(record,
                (metadata, exception) -> {
                    if (exception != null) {
                        em.fail(exception);
                    } else {
                        em.complete(metadata);
                    }
                })).emitOn(getOrCreateExecutor())
                .invoke(() -> LOGGER.debugf("Producer %s: sent message %s", clientId(), record));
    }

    Multi<RecordMetadata> getProduceMulti(Multi<ProducerRecord<K, V>> recordProducer) {
        return Multi.createFrom().deferred(() -> {
            if (isTransactional()) {
                getOrCreateProducer().beginTransaction();
            }

            MultiFlatten<ProducerRecord<K, V>, RecordMetadata> flatten = recordProducer.onItem()
                    .transformToUni(this::record);
            Multi<RecordMetadata> multi = (concurrency <= 1) ? flatten.concatenate() : flatten.merge(concurrency);
            return multi
                    .runSubscriptionOn(getOrCreateExecutor())
                    .onTermination()
                    .invoke((throwable, cancelled) -> onTermination.accept(getOrCreateProducer(), throwable));
        });
    }

    /**
     * Create {@link ProducerTask} for creating records reading from a comma-separated classpath resource.
     * <p>
     * This {@link ProducerBuilder} needs to be created providing {@link Serde}s to deserialize keys and values from the
     * comma-separated file.
     *
     * @param resourcePath the path for the resource in the classpath
     * @return the {@link ProducerTask}
     */
    public ProducerTask fromCsv(String resourcePath) {
        Objects.requireNonNull(resourcePath, "resource path");
        Objects.requireNonNull(keySerde, "Producer needs to be created with key Serde");
        Objects.requireNonNull(valueSerde, "Producer needs to be created with value Serde");
        return new ProducerTask(getProduceMulti(
                Multi.createFrom()
                        .resource(() -> new CSVReader(new InputStreamReader(getResourceAsStream(resourcePath), UTF_8)),
                                reader -> Multi.createFrom().iterable(reader)
                                        .onItem().transform(this::getProducerRecord)
                                        .filter(Objects::nonNull))
                        .withFinalizer(Unchecked.consumer(CSVReader::close))));
    }

    private ProducerRecord<K, V> getProducerRecord(String[] values) {
        if (values.length < 1)
            return null;
        if (values.length == 1) { // topic, null
            return new ProducerRecord<>(values[0], null);
        } else if (values.length == 2) { // topic, value
            return new ProducerRecord<>(values[0], // topic
                    valueSerde.deserializer().deserialize(values[0], values[1].getBytes(UTF_8))); //value
        } else if (values.length == 3) { // topic, key, value
            return new ProducerRecord<>(values[0], //topic
                    keySerde.deserializer().deserialize(values[0], values[1].getBytes(UTF_8)), // key
                    valueSerde.deserializer().deserialize(values[0], values[1].getBytes(UTF_8))); // value
        } else { // topic, partition, key, value
            return new ProducerRecord<>(values[0], // topic
                    Integer.parseInt(values[1]), // partition
                    keySerde.deserializer().deserialize(values[0], values[2].getBytes(UTF_8)), // key
                    valueSerde.deserializer().deserialize(values[0], values[3].getBytes(UTF_8))); // value
        }
    }

    private InputStream getResourceAsStream(String resourcePath) {
        final Set<ClassLoader> classLoadersToSearch = new HashSet<>();
        // try context and system classloaders as well
        classLoadersToSearch.add(Thread.currentThread().getContextClassLoader());
        classLoadersToSearch.add(ClassLoader.getSystemClassLoader());
        classLoadersToSearch.add(KafkaCompanion.class.getClassLoader());

        for (final ClassLoader classLoader : classLoadersToSearch) {
            InputStream stream = classLoader.getResourceAsStream(resourcePath);
            if (stream != null) {
                return stream;
            }

            // Be lenient if an absolute path was given
            if (resourcePath.startsWith("/")) {
                stream = classLoader.getResourceAsStream(resourcePath.replaceFirst("/", ""));
                if (stream != null) {
                    return stream;
                }
            }
        }
        throw new IllegalArgumentException("Resource '" + resourcePath + "' not found on classpath.");
    }

    /**
     * Create {@link ProducerTask} for creating records from the given {@link Multi}.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     *
     * @param recordMulti the multi providing {@link ProducerRecord}s to produce.
     * @return the {@link ProducerTask}
     */
    public ProducerTask fromMulti(Multi<ProducerRecord<K, V>> recordMulti) {
        Objects.requireNonNull(recordMulti, "record multi");
        return new ProducerTask(getProduceMulti(recordMulti));
    }

    /**
     * Create {@link ProducerTask} for creating records from the given {@link List} of records.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     *
     * @param records the list of {@link ProducerRecord}s to produce.
     * @return the {@link ProducerTask}
     */
    public ProducerTask fromRecords(List<ProducerRecord<K, V>> records) {
        Objects.requireNonNull(records, "records");
        return fromMulti(Multi.createFrom().iterable(records));
    }

    /**
     * Create {@link ProducerTask} for creating records from the given array of records.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     *
     * @param records the array of {@link ProducerRecord} to produce.
     * @return the {@link ProducerTask}
     */
    @SafeVarargs
    public final ProducerTask fromRecords(ProducerRecord<K, V>... records) {
        Objects.requireNonNull(records, "records");
        return fromMulti(Multi.createFrom().items(records));
    }

    /**
     * Create {@link ProducerTask} for creating records generated using the given function.
     * <p>
     * The plug function is used to modify the {@link Multi} generating the records, ex. limiting the number of records
     * produced.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     * The task will run until the {@link Multi} is terminated (with completion or failure) or
     * is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param generatorFunction the function to generate {@link ProducerRecord}s
     * @param plugFunction the function to apply to the resulting multi
     * @return the {@link ProducerTask}
     */
    public ProducerTask usingGenerator(Function<Integer, ProducerRecord<K, V>> generatorFunction,
            Function<Multi<ProducerRecord<K, V>>, Multi<ProducerRecord<K, V>>> plugFunction) {
        Objects.requireNonNull(generatorFunction, "record generator function");
        return fromMulti(Multi.createFrom().range(0, Integer.MAX_VALUE)
                .onItem().transform(generatorFunction).plug(plugFunction));
    }

    /**
     * Create {@link ProducerTask} for creating records from the given array of records.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     * The task will run until the {@link Multi} is terminated (with completion or failure) or
     * is explicitly stopped (subscription to the {@link Multi} cancelled).
     *
     * @param generatorFunction the function to generate {@link ProducerRecord}s
     * @return the {@link ProducerTask}
     */
    public ProducerTask usingGenerator(Function<Integer, ProducerRecord<K, V>> generatorFunction) {
        return usingGenerator(generatorFunction, Function.identity());
    }

    /**
     * Create {@link ProducerTask} for creating records from the given array of records.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     * The task will run for generating the given number of records.
     *
     * @param generatorFunction the function to generate {@link ProducerRecord}s
     * @param numberOfRecords the number of records to produce
     * @return the {@link ProducerTask}
     */
    public ProducerTask usingGenerator(Function<Integer, ProducerRecord<K, V>> generatorFunction, long numberOfRecords) {
        return usingGenerator(generatorFunction, until(numberOfRecords));
    }

    /**
     * Create {@link ProducerTask} for creating records from the given array of records.
     * <p>
     * The resulting {@link ProducerTask} will be already started.
     * The task will run during the given duration.
     *
     * @param generatorFunction the function to generate {@link ProducerRecord}s
     * @param during the duration of the producing task to run
     * @return the {@link ProducerTask}
     */
    public ProducerTask usingGenerator(Function<Integer, ProducerRecord<K, V>> generatorFunction, Duration during) {
        return usingGenerator(generatorFunction, until(during));
    }
}
