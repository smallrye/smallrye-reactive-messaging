package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.ConfigurationCleaner;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSink;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ClientTestBase extends KafkaCompanionTestBase {

    public static final int DEFAULT_TEST_TIMEOUT = 60_000;

    protected String topic;
    protected final int partitions = 4;
    protected long receiveTimeoutMillis = DEFAULT_TEST_TIMEOUT;
    protected final long requestTimeoutMillis = 3000;
    protected final long sessionTimeoutMillis = 12000;

    private final List<List<Integer>> expectedMessages = new ArrayList<>(partitions);
    protected final List<List<Integer>> receivedMessages = new ArrayList<>(partitions);

    final Semaphore assignSemaphore = new Semaphore(partitions);
    final List<Cancellable> subscriptions = new ArrayList<>();
    KafkaSource<Integer, String> source;

    protected Queue<KafkaSink> sinks;

    protected Queue<KafkaSource<Integer, String>> sources;

    @AfterEach
    public void tearDown() {
        cancelSubscriptions();
        for (KafkaSource<Integer, String> source : sources) {
            source.closeQuietly();
        }
        for (KafkaSink sink : sinks) {
            sink.closeQuietly();
        }
    }

    @BeforeEach
    public void init() {
        sources = new ConcurrentLinkedQueue<>();
        sinks = new ConcurrentLinkedQueue<>();

        String newTopic = "test-" + UUID.randomUUID();
        companion.topics().createAndWait(newTopic, partitions);
        this.topic = newTopic;
        resetMessages();
    }

    private long committedCount(ReactiveKafkaConsumer<Integer, String> client) {
        TopicPartition[] tps = IntStream.range(0, partitions)
                .mapToObj(i -> new TopicPartition(topic, i)).distinct().toArray(TopicPartition[]::new);
        Map<TopicPartition, OffsetAndMetadata> map = client
                .committed(tps)
                .await()
                .atMost(Duration.ofSeconds(receiveTimeoutMillis));

        return map.values().stream()
                .filter(offset -> offset != null && offset.offset() > 0)
                .mapToLong(OffsetAndMetadata::offset)
                .sum();
    }

    void waitForCommits(KafkaSource<Integer, String> source, int count) {
        ReactiveKafkaConsumer<Integer, String> client = source.getConsumer();
        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> assertThat(committedCount(client)).isEqualTo(count));
    }

    public KafkaSource<Integer, String> createSource() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        return createSource(config, groupId);
    }

    public KafkaSource<Integer, String> createSource(MapBasedConfig config, String groupId) {
        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignation());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config), commitHandlerFactories,
                failureHandlerFactories,
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        sources.add(source);
        return source;
    }

    public KafkaSource<Integer, String> createSourceSeekToBeginning() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToBeginning());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config), commitHandlerFactories,
                failureHandlerFactories,
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        sources.add(source);
        return source;
    }

    public KafkaSource<Integer, String> createSourceSeekToEnd() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToEnd());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                commitHandlerFactories, failureHandlerFactories,
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        sources.add(source);
        return source;
    }

    public KafkaSource<Integer, String> createSourceSeekToOffset() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToOffset());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                commitHandlerFactories, failureHandlerFactories,
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        sources.add(source);
        return source;
    }

    public KafkaSource<Integer, String> createSource(MapBasedConfig config, int index) {
        source = new KafkaSource<>(vertx, "groupId", new KafkaConnectorIncomingConfiguration(config),
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), index);
        sources.add(source);
        return source;
    }

    KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignation() {
        return new KafkaConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                ClientTestBase.this.onPartitionsAssigned(partitions);
            }
        };
    }

    KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToBeginning() {
        return new KafkaConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                consumer.seekToBeginning(partitions);
                assertThat(topic).isEqualTo(partitions.iterator().next().topic());
                assignSemaphore.release(partitions.size());
            }
        };
    }

    KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToEnd() {
        return new KafkaConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                consumer.seekToEnd(partitions);
                assertThat(topic).isEqualTo(partitions.iterator().next().topic());
                assignSemaphore.release(partitions.size());
            }
        };
    }

    KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToOffset() {
        return new KafkaConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                partitions.forEach(tp -> consumer.seek(tp, 1));
                assertThat(topic).isEqualTo(partitions.iterator().next().topic());
                assignSemaphore.release(partitions.size());
            }
        };
    }

    void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) { // If no assignments, ignore callback
            assertThat(topic).isEqualTo(partitions.iterator().next().topic());
            assignSemaphore.release(partitions.size());
        }
    }

    void waitForMessages(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS)) {
            fail(latch.getCount() + " messages not received, received=" + count(receivedMessages) + " : "
                    + receivedMessages);
        }
    }

    void subscribe(Multi<IncomingKafkaRecord<Integer, String>> stream, CountDownLatch... latches)
            throws Exception {
        Cancellable cancellable = stream
                .onItem().invoke(record -> {
                    onReceive(record);
                    for (CountDownLatch latch : latches) {
                        latch.countDown();
                    }
                })
                .subscribe().with(ignored -> {
                    // Ignored.
                });
        subscriptions.add(cancellable);
        waitForPartitionAssignment();
    }

    void waitForPartitionAssignment() throws InterruptedException {
        Assertions.assertTrue(
                assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS), "Partitions not assigned");
    }

    void sendMessages(int startIndex, int count) throws Exception {
        sendMessages(IntStream.range(0, count).mapToObj(i -> createProducerRecord(startIndex + i, true)));
    }

    void sendMessages(int startIndex, int count, String broker) throws Exception {
        sendMessages(IntStream.range(0, count).mapToObj(i -> new ProducerRecord<>(topic, 0, i, "Message " + i)), broker);
    }

    void sendMessages(Stream<? extends ProducerRecord<Integer, String>> records) throws Exception {
        Map<String, Object> configs = producerProps();
        ConfigurationCleaner.cleanupProducerConfiguration(configs);
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs)) {
            List<Future<?>> futures = records.map(producer::send).collect(Collectors.toList());

            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            producer.flush();
        }
    }

    void sendMessages(Stream<? extends ProducerRecord<Integer, String>> records, String broker) throws Exception {
        Map<String, Object> configs = producerProps();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        ConfigurationCleaner.cleanupProducerConfiguration(configs);
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs)) {
            List<Future<?>> futures = records.map(producer::send).collect(Collectors.toList());
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            producer.flush();
        }
    }

    public Map<String, Object> producerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put("tracing-enabled", false);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 5);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMillis));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public MapBasedConfig createProducerConfig() {
        return new MapBasedConfig(producerProps());
    }

    protected MapBasedConfig createConsumerConfig(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put("channel-name", "test");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, companion.getBootstrapServers());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(sessionTimeoutMillis));
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(3000));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + groupId);
        return new MapBasedConfig(props);
    }

    protected ProducerRecord<Integer, String> createProducerRecord(int index, boolean expectSuccess) {
        int partition = index % partitions;
        if (expectSuccess) {
            expectedMessages.get(partition).add(index);
        }
        return new ProducerRecord<>(topic, partition, index, "Message " + index);
    }

    protected Multi<ProducerRecord<Integer, String>> createProducerRecords(int count) {
        return Multi.createFrom().range(0, count).map(i -> createProducerRecord(i, true));
    }

    protected void onReceive(IncomingKafkaRecord<Integer, String> record) {
        receivedMessages.get(record.getPartition()).add(record.getKey());
    }

    protected void onReceive(IncomingKafkaRecordBatch<Integer, String> record) {
        for (KafkaRecord<Integer, String> kafkaRecord : record) {
            receivedMessages.get(kafkaRecord.getPartition()).add(kafkaRecord.getKey());
        }
    }

    protected void checkConsumedMessages() {
        assertThat(receivedMessages).hasSize(expectedMessages.size());
        assertThat(receivedMessages).containsExactlyElementsOf(expectedMessages);
    }

    protected void checkConsumedMessages(int receiveStartIndex, int receiveCount) {
        for (int i = 0; i < partitions; i++) {
            checkConsumedMessages(i, receiveStartIndex, receiveStartIndex + receiveCount - 1);
        }
    }

    protected void checkConsumedMessages(int partition, int receiveStartIndex, int receiveEndIndex) {
        List<Integer> expected = new ArrayList<>(expectedMessages.get(partition));
        expected.removeIf(index -> index < receiveStartIndex || index > receiveEndIndex);
        assertThat(receivedMessages.get(partition)).containsExactlyElementsOf(expected);
    }

    protected int count(List<List<Integer>> list) {
        return list.stream().mapToInt(List::size).sum();
    }

    protected void resetMessages() {
        expectedMessages.clear();
        receivedMessages.clear();
        for (int i = 0; i < partitions; i++) {
            expectedMessages.add(new CopyOnWriteArrayList<>());
        }
        for (int i = 0; i < partitions; i++) {
            receivedMessages.add(new CopyOnWriteArrayList<>());
        }
    }

    protected String createNewTopicWithPrefix(String prefix) {
        String newTopic = prefix + "-" + System.nanoTime();
        companion.topics().createAndWait(newTopic, partitions);
        resetMessages();
        return newTopic;
    }

    protected void clearReceivedMessages() {
        receivedMessages.forEach(List::clear);
    }

    protected void cancelSubscriptions() {
        subscriptions.forEach(Cancellable::cancel);
        subscriptions.clear();
    }
}
