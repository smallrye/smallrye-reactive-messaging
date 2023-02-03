package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.kafka.companion.ProducerTask;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class FileCheckpointStateStoreTest extends KafkaCompanionTestBase {

    private KafkaSource<String, Integer> source;
    private KafkaSource<String, Integer> source2;
    private String groupId;

    @BeforeEach
    void setUp() {
        groupId = UUID.randomUUID().toString();
    }

    @AfterEach
    public void stopAll() {
        if (source != null) {
            source.closeQuietly();
        }
        if (source2 != null) {
            source2.closeQuietly();
        }
    }

    private void checkOffsetSum(File tempDir, int sum) {
        await().untilAsserted(() -> {
            List<JsonObject> states = getStateFromStore(tempDir, 3);

            int offset = states.stream().mapToInt(tuple -> tuple.getInteger("offset")).sum();
            int state = states.stream().mapToInt(tuple -> tuple.getInteger("state")).sum();
            System.out.println(states.stream().map(JsonObject::toString).collect(Collectors.joining(", "))
                    + " : " + offset + " " + state);

            assertThat(offset).isEqualTo(sum);
            assertThat(state).isEqualTo(sum(sum));
        });
    }

    private List<JsonObject> getStateFromStore(File tempDir, int partitions) {
        return Uni.join().all(IntStream.range(0, partitions).boxed()
                .map(i -> tempDir.toPath().resolve(groupId + ":" + topic + ":" + i).toString())
                .map(path -> vertx.fileSystem().readFile(path)
                        .map(Buffer::toJsonObject)
                        .onFailure().recoverWithItem(JsonObject.of("offset", 0, "state", 0)))
                .collect(Collectors.toList()))
                .andFailFast()
                .await().indefinitely();
    }

    private int sum(int sum) {
        return sum * (sum - 1) / 2;
    }

    @Test
    public void testMultipleIndependentConsumers(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = newCommonConfigForSource()
                .with("group.id", groupId)
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);

        SingletonInstance<KafkaCommitHandler.Factory> checkpointFactory = new SingletonInstance<>("checkpoint",
                new KafkaCheckpointCommit.Factory(new SingletonInstance<>("file",
                        new FileCheckpointStateStore.Factory(UnsatisfiedInstance.instance()))));
        source = new KafkaSource<>(vertx,
                groupId,
                ic,
                checkpointFactory,
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().onItem().transformToUniAndConcatenate(m -> {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(m);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + m.getPayload(), true);
            }
            messages.add(m);
            return Uni.createFrom().completionStage(m.ack());
        }).subscribe().with(unused -> {
        });

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i), i), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() >= 100);
        checkOffsetSum(tempDir, 100);

        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(
                config.with(ConsumerConfig.CLIENT_ID_CONFIG,
                        source.getConsumer().get(ConsumerConfig.CLIENT_ID_CONFIG) + "-2"));
        source2 = new KafkaSource<>(vertx,
                groupId,
                ic2,
                checkpointFactory,
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().onItem().transformToUniAndConcatenate(m -> {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(m);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + m.getPayload(), true);
            }
            messages2.add(m);
            return Uni.createFrom().completionStage(m.ack());
        }).subscribe().with(x -> {
        });

        await().until(() -> !source2.getConsumer().getAssignments().await().indefinitely().isEmpty());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i + 100), i + 100), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 200);
        checkOffsetSum(tempDir, 200);

        source.closeQuietly();
        await().until(() -> source2.getConsumer().getAssignments().await().indefinitely().size() == 3);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i + 200), i + 200), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 300);
        checkOffsetSum(tempDir, 300);
    }

    @Test
    public void testWithPartitions(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        RemoteStoringBean application = runApplication(config, RemoteStoringBean.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testWithPartitionsBlocking(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("checkpoint.unsynced-state-max-age.ms", 60000)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        RemoteStoringBlockingBean application = runApplication(config, RemoteStoringBlockingBean.class);

        int expected = 1000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testWithPartitionsStoreLocally(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        LocalStoringBean application = runApplication(config, LocalStoringBean.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testWithNullState(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("auto.commit.interval.ms", 500)
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        NullStateBean application = runApplication(config, NullStateBean.class);

        int expected = 3000;
        Random random = new Random();
        ProducerTask recordMetadata = companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);
        assertThat(application.getReceived().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));

        await().untilAsserted(() -> {
            List<JsonObject> state = getStateFromStore(tempDir, 3);
            recordMetadata.byTopicPartition().forEach((tp, records) -> {
                JsonObject partitionState = state.get(tp.partition());
                long lastPublishedOffset = records.get(records.size() - 1).offset();
                assertThat(partitionState.getInteger("offset")).isEqualTo(lastPublishedOffset + 1);
            });
        });
    }

    @Test
    public void testFailingBeanWithIgnoredFailure(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("failure-strategy", "ignore")
                .with("max.poll.records", 10)
                .with("checkpoint.unsynced-state-max-age.ms", 600)
                .with("auto.commit.interval.ms", 500)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        runApplication(config, FailingBean.class);

        int expected = 10;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> !getHealth().getLiveness().isOk());
    }

    @Test
    public void testSelectivelyFailingBean(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("failure-strategy", "ignore")
                .with("max.poll.records", 10)
                .with("checkpoint.unsynced-state-max-age.ms", 600)
                .with("checkpoint.state-store", "file")
                .with("auto.commit.interval.ms", 500)
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        SelectivelyFailingBean application = runApplication(config, SelectivelyFailingBean.class);

        int expected = 200;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 0, Integer.toString(i), i), expected);
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 1, Integer.toString(i), i), expected);
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 2, Integer.toString(i), i), expected);

        await()
                .atMost(1, TimeUnit.MINUTES) //19900
                .until(() -> application.count() >= (expected - 20) * 3);

        await().untilAsserted(() -> {
            List<JsonObject> states = getStateFromStore(tempDir, 3);

            int offset = states.stream().mapToInt(tuple -> tuple.getInteger("offset")).sum();
            int state = states.stream().mapToInt(tuple -> tuple.getInteger("state")).sum();
            System.out.println(states.stream().map(JsonObject::toString).collect(Collectors.joining(", "))
                    + " : " + offset + " " + state);

            assertThat(offset).isEqualTo(expected * 3);
            assertThat(state).isEqualTo((sum(expected) - sum(20) * 10) * 3);
        });
    }

    @Test
    public void testWithPreviousState(@TempDir File tempDir) {
        vertx.fileSystem().writeFile(tempDir.toPath().resolve(groupId + ":" + topic + ":" + 0).toString(),
                Buffer.newInstance(JsonObject.of("offset", 500, "state", sum(500)).toBuffer()))
                .await().indefinitely();

        int expected = 1000;
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i), i), expected)
                .awaitCompletion(Duration.ofMinutes(1));

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        RemoteStoringBean application = runApplication(config, RemoteStoringBean.class);

        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= 500);

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testWaitAfterAssignment(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 1);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("checkpoint.unsynced-state-max-age.ms", 500)
                .with("auto.commit.interval.ms", 200)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        RemoteStoringBean application = runApplication(config, RemoteStoringBean.class);

        // consumer assigned partitions but receives no records
        await().untilAsserted(() -> {
            assertThat(getHealth().getReadiness().getChannels()).isNotEmpty();
            assertThat(getHealth().getReadiness().isOk()).isTrue();
        });

        int expected = 100;
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), expected)
                .awaitCompletion(Duration.ofMinutes(1));

        await().until(() -> application.count() >= expected);

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testGracefulTerminationWaitForProcessing(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("graceful-shutdown", true)
                .with("commit-strategy", "checkpoint")
                .with("auto.commit.interval.ms", 30000)
                .with("max.poll.records", 10)
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("checkpoint.unsynced-state-max-age.ms", 60000)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        RemoteStoringBlockingBean application = runApplication(config, RemoteStoringBlockingBean.class);

        int expected = 200;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 0, Integer.toString(i), i), expected);
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 1, Integer.toString(i), i), expected);
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, 2, Integer.toString(i), i), expected);

        // terminate the connector
        getBeanManager().createInstance()
                .select(KafkaConnector.class, ConnectorLiteral.of("smallrye-kafka")).get().terminate(new Object());

        await()
                .atMost(1, TimeUnit.MINUTES)
                .pollDelay(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<JsonObject> states = getStateFromStore(tempDir, 3);
                    for (JsonObject json : states) {
                        int offset = json.getInteger("offset");
                        int state = json.getInteger("state");
                        System.out.println(json);
                        assertThat(state).isEqualTo(sum(offset));
                    }
                });
    }

    @Test
    public void testProcessorBean(@TempDir File tempDir) {
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("auto.commit.interval.ms", 500)
                .with("checkpoint.unsynced-state-max-age.ms", 60000)
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("mp.messaging.outgoing.sink")
                .with("connector", "smallrye-kafka")
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("topic", topic + "-sink")
                .with("max-inflight-messages", 5) // limit outgoing records in parallel, each one will try to persist state in redis
        ;

        RemoteStoringProcessorBean application = runApplication(config, RemoteStoringProcessorBean.class);

        int expected = 1000;
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i), i), expected)
                .awaitCompletion(Duration.ofMinutes(1));

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(application.count()).isGreaterThanOrEqualTo(expected));

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testProcessorBeanWithPartitions(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);
        companion.topics().createAndWait(topic + "-sink", 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("auto.commit.interval.ms", 500)
                .with("checkpoint.unsynced-state-max-age.ms", 60000)
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("mp.messaging.outgoing.sink")
                .with("connector", "smallrye-kafka")
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("topic", topic + "-sink")
                .with("max-inflight-messages", 5) // limit outgoing records in parallel, each one will try to persist state in redis
        ;

        RemoteStoringProcessorBean application = runApplication(config, RemoteStoringProcessorBean.class);

        int expected = 1000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(i), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(application.count()).isGreaterThanOrEqualTo(expected));

        ConsumerTask<String, Integer> consumerRecords = companion.consumeIntegers()
                .fromTopics(topic + "-sink", expected);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            System.out.println(consumerRecords.byTopicPartition().entrySet().stream()
                    .map(e -> {
                        List<ConsumerRecord<String, Integer>> records = e.getValue();
                        return e.getKey().partition() + ":" + records.get(records.size() - 1).offset();
                    }).collect(Collectors.joining(",")));
            assertThat(consumerRecords.count()).isEqualTo(expected);
        });

        checkOffsetSum(tempDir, expected);
    }

    @Test
    public void testCustomStateBeanWithPartitions(@TempDir File tempDir) {
        addBeans(DatabindProcessingStateCodec.Factory.class);
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint")
                .with("checkpoint.state-store", "file")
                .with("checkpoint.state-type", MyState.class.getName())
                .with("auto.commit.interval.ms", 500)
                .with("checkpoint.unsynced-state-max-age.ms", 60000)
                .with("checkpoint.file.state-dir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        CustomStateBean application = runApplication(config, CustomStateBean.class);

        int expected = 1000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(i), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(application.count()).isGreaterThanOrEqualTo(expected));

        await().untilAsserted(() -> {
            List<JsonObject> states = getStateFromStore(tempDir, 3);

            int offset = states.stream().mapToInt(tuple -> tuple.getInteger("offset")).sum();
            int state = states.stream().mapToInt(tuple -> tuple.getJsonObject("state").getInteger("counter")).sum();
            System.out.println(states.stream().map(JsonObject::toString).collect(Collectors.joining(", "))
                    + " : " + offset + " " + state);

            assertThat(offset).isEqualTo(expected);
            assertThat(state).isEqualTo(sum(expected));
        });
    }

    @ApplicationScoped
    public static class RemoteStoringBlockingBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Blocking
        public CompletionStage<Void> consume(Message<Integer> msg) throws InterruptedException {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            Thread.sleep(10);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + msg.getPayload(), true);
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class RemoteStoringBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + msg.getPayload(), true);
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class LocalStoringBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(0, current -> current + msg.getPayload());
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class FailingBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public void consume(Integer msg) {
            throw new IllegalArgumentException("boom");
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class SelectivelyFailingBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                if (checkpointMetadata.getRecordOffset() % 10 == 0) {
                    return msg.nack(new IllegalArgumentException("boom"));
                }
                checkpointMetadata.transform(0, current -> current + msg.getPayload());
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class NullStateBean {

        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                checkpointMetadata.setNext(null);
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class RemoteStoringProcessorBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Outgoing("sink")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Message<Integer> process(Message<Integer> msg) {
            CheckpointMetadata<Integer> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            Integer newPayload = msg.getPayload();
            if (checkpointMetadata != null) {
                newPayload = checkpointMetadata.transform(0, c -> c + msg.getPayload(), true);
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            IncomingKafkaRecordMetadata<?, ?> metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).get();
            return msg.withPayload(newPayload)
                    .addMetadata(OutgoingKafkaRecordMetadata.builder()
                            .withPartition(metadata.getPartition())
                            .build());
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class CustomStateBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> process(Message<Integer> msg) {
            CheckpointMetadata<MyState> checkpointMetadata = CheckpointMetadata.fromMessage(msg);
            if (checkpointMetadata != null) {
                checkpointMetadata.transform(() -> new MyState(checkpointMetadata.getTopicPartition().toString()),
                        c -> c.addtoCounter(msg.getPayload()), true);
            }
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    static class MyState {
        AtomicInteger counter;
        String name;

        public MyState() {
        }

        public MyState(String name) {
            this.counter = new AtomicInteger();
            this.name = name;
        }

        public MyState addtoCounter(int toAdd) {
            counter.addAndGet(toAdd);
            return this;
        }

        public AtomicInteger getCounter() {
            return counter;
        }

        public void setCounter(AtomicInteger counter) {
            this.counter = counter;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    private int getMaxNumberOfEventLoop(int expected) {
        // On Github Actions, only one event loop is created.
        return Math.min(expected, Runtime.getRuntime().availableProcessors() / 2);
    }

}
