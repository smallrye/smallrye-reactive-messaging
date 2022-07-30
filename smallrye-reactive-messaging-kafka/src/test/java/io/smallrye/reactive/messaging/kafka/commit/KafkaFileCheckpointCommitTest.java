package io.smallrye.reactive.messaging.kafka.commit;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class KafkaFileCheckpointCommitTest extends KafkaCompanionTestBase {

    private KafkaSource<String, Integer> source;
    private KafkaSource<String, Integer> source2;

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
        await().until(() -> {
            List<JsonObject> states = Uni.join().all(Stream.of(0, 1, 2)
                    .map(i -> tempDir.toPath().resolve(topic + "-" + i).toString())
                    .map(path -> vertx.fileSystem().readFile(path).map(Buffer::toJsonObject))
                    .collect(Collectors.toList()))
                    .andFailFast()
                    .await().indefinitely();

            int offset = states.stream().mapToInt(tuple -> tuple.getInteger("offset")).sum();
            int state = states.stream().mapToInt(tuple -> tuple.getInteger("state")).sum();

            return offset == sum && state == sum * (sum - 1) / 2;
        });
    }

    @Test
    public void testMultipleIndependentConsumers(@TempDir File tempDir) {
        companion.topics().createAndWait(topic, 3);

        MapBasedConfig config = newCommonConfigForSource()
                .with("group.id", "test-source-with-auto-commit-enabled")
                .with("commit-strategy", "checkpoint-file")
                .with("checkpoint-file.stateDir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);

        source = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic,
                new SingletonInstance<>("checkpoint-file", new KafkaFileCheckpointCommit.Factory()),
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(m -> {
            StateStore<Integer> stateStore = StateStore.fromMessage(m);
            if (stateStore != null) {
                stateStore.transformAndStoreOnAck(0, current -> current + m.getPayload());
            }
            messages.add(m);
            Uni.createFrom().completionStage(m.ack())
                    .runSubscriptionOn(vertx::runOnContext)
                    .subscribeAsCompletionStage();
        });

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() >= 100);
        checkOffsetSum(tempDir, 100);

        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(
                config.with(ConsumerConfig.CLIENT_ID_CONFIG,
                        source.getConsumer().get(ConsumerConfig.CLIENT_ID_CONFIG) + "-2"));
        source2 = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic2,
                new SingletonInstance<>("checkpoint-file", new KafkaFileCheckpointCommit.Factory()),
                failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().subscribe().with(m -> {
            StateStore<Integer> stateStore = StateStore.fromMessage(m);
            if (stateStore != null) {
                stateStore.transformAndStoreOnAck(0, current -> current + m.getPayload());
            }
            messages2.add(m);
            Uni.createFrom().completionStage(m.ack())
                    .runSubscriptionOn(vertx::runOnContext)
                    .subscribeAsCompletionStage();
        });

        await().until(() -> !source2.getConsumer().getAssignments().await().indefinitely().isEmpty());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i + 100), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 200);
        checkOffsetSum(tempDir, 200);

        source.closeQuietly();
        await().until(() -> source2.getConsumer().getAssignments().await().indefinitely().size() == 3);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i + 200), 100);
        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() + messages2.size() >= 300);
        checkOffsetSum(tempDir, 300);
    }

    @Test
    public void testWithPartitions(@TempDir File tempDir) {
        System.out.println(tempDir.getAbsolutePath());

        addBeans(KafkaFileCheckpointCommit.Factory.class);
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "checkpoint-file")
                .with("checkpoint-file.stateDir", tempDir.getAbsolutePath())
                .with("value.deserializer", IntegerDeserializer.class.getName());

        MyApplication application = runApplication(config, MyApplication.class);

        int expected = 1000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() == expected);

        checkOffsetSum(tempDir, expected);
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

}
