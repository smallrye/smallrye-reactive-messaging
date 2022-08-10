package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.redis.client.Command;
import io.vertx.mutiny.redis.client.Redis;
import io.vertx.mutiny.redis.client.Request;
import io.vertx.mutiny.redis.client.Response;
import io.vertx.redis.client.RedisOptions;

@Tag(TestTags.FLAKY)
@Disabled
public class KafkaRedisCheckpointCommitTest extends KafkaCompanionTestBase {

    private KafkaSource<String, Integer> source;
    private KafkaSource<String, Integer> source2;

    public static GenericContainer<?> redis;
    private Redis redisClient;

    @BeforeAll
    static void beforeAll() {
        redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redis.start();
        await().until(() -> redis.isRunning());
    }

    @AfterAll
    static void afterAll() {
        if (redis != null) {
            redis.stop();
        }
    }

    static String getRedisString() {
        return String.format("redis://%s:%d", redis.getHost(), redis.getMappedPort(6379));
    }

    @BeforeEach
    void setUp() {
        redisClient = Redis.createClient(vertx, new RedisOptions()
                .addConnectionString(getRedisString()))
                .connectAndForget();
    }

    @AfterEach
    public void stopAll() {
        if (source != null) {
            source.closeQuietly();
        }
        if (source2 != null) {
            source2.closeQuietly();
        }
        redisClient.close();
    }

    private void checkOffsetSum(int sum) {
        await().until(() -> {
            List<JsonObject> states = Uni.join().all(Stream.of(0, 1, 2)
                    .map(i -> redisClient.send(Request.cmd(Command.GET).arg(topic + ":" + i))
                            .map(r -> Optional.ofNullable(r)
                                    .map(Response::toBuffer)
                                    .map(Buffer::toJsonObject)
                                    .orElse(JsonObject.of("offset", 0, "state", 0))))
                    .collect(Collectors.toList()))
                    .andFailFast()
                    .await().indefinitely();

            int offset = states.stream().mapToInt(tuple -> tuple.getInteger("offset")).sum();
            int state = states.stream().mapToInt(tuple -> tuple.getInteger("state")).sum();
            System.out.println(states.stream().map(JsonObject::toString).collect(Collectors.joining(", "))
                    + " : " + offset + " " + state);

            return offset == sum && state == sum(sum);
        });
    }

    private int sum(int sum) {
        return sum * (sum - 1) / 2;
    }

    @Test
    public void testMultipleIndependentConsumers() {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = newCommonConfigForSource()
                .with("group.id", "test-source-with-auto-commit-enabled")
                .with("commit-strategy", "checkpoint-redis")
                .with("checkpoint-redis.connectionString", getRedisString())
                .with("checkpoint-redis.maxPoolSize", 30)
                .with("checkpoint-redis.maxWaitingHandlers", 1024)
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);

        source = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic,
                new SingletonInstance<>("checkpoint-redis", new KafkaRedisCheckpointCommit.Factory()),
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().onItem().transformToUniAndConcatenate(m -> {
            StateStore<Integer> stateStore = StateStore.fromMessage(m);
            if (stateStore != null) {
                stateStore.transformAndStoreOnAck(0, current -> current + m.getPayload());
            }
            messages.add(m);
            return Uni.createFrom().completionStage(m.ack());
        }).subscribe().with(unused -> {
        });

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() >= 100);
        checkOffsetSum(100);

        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(
                config.with(ConsumerConfig.CLIENT_ID_CONFIG,
                        source.getConsumer().get(ConsumerConfig.CLIENT_ID_CONFIG) + "-2"));
        source2 = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic2,
                new SingletonInstance<>("checkpoint-redis", new KafkaRedisCheckpointCommit.Factory()),
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().onItem().transformToUniAndConcatenate(m -> {
            StateStore<Integer> stateStore = StateStore.fromMessage(m);
            if (stateStore != null) {
                stateStore.transformAndStoreOnAck(0, current -> current + m.getPayload());
            }
            messages2.add(m);
            return Uni.createFrom().completionStage(m.ack());
        }).subscribe().with(x -> {
        });

        await().until(() -> !source2.getConsumer().getAssignments().await().indefinitely().isEmpty());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i + 100), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 200);
        checkOffsetSum(200);

        source.closeQuietly();
        await().until(() -> source2.getConsumer().getAssignments().await().indefinitely().size() == 3);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i + 200), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 300);
        checkOffsetSum(300);
    }

    @Test
    public void testWithPartitions() {
        addBeans(KafkaRedisCheckpointCommit.Factory.class);
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint-redis")
                .with("checkpoint-redis.connectionString", getRedisString())
                .with("checkpoint-redis.maxPoolSize", 30)
                .with("checkpoint-redis.maxWaitingHandlers", 1024)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

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

        checkOffsetSum(expected);
    }

    @Test
    public void testWithPreviousState() {
        addBeans(KafkaRedisCheckpointCommit.Factory.class);
        String groupId = UUID.randomUUID().toString();

        redisClient.send(Request.cmd(Command.SET)
                .arg(topic + ":" + 0)
                .arg(JsonObject.of("offset", 500, "state", sum(500)).toBuffer().toString()))
                .await().indefinitely();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint-redis")
                .with("checkpoint-redis.connectionString", getRedisString())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        int expected = 1000;
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i), i), expected)
                .awaitCompletion(Duration.ofMinutes(1));

        await()
                .atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);

        checkOffsetSum(expected);
    }

    @ApplicationScoped
    public static class MyApplication {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            StateStore<Integer> stateStore = StateStore.fromMessage(msg);
            if (stateStore != null) {
                stateStore.transformAndStoreOnAck(0, current -> current + msg.getPayload());
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

    private int getMaxNumberOfEventLoop(int expected) {
        // On Github Actions, only one event loop is created.
        return Math.min(expected, Runtime.getRuntime().availableProcessors() / 2);
    }
}
