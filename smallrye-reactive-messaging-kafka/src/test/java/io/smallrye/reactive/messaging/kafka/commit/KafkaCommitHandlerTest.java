package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaAdminHelper;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@SuppressWarnings("unchecked")
public class KafkaCommitHandlerTest extends KafkaTestBase {

    Admin admin;
    private KafkaSource<String, Integer> source;

    @AfterEach
    public void stopAll() {
        if (source != null) {
            source.closeQuietly();
        }

        if (admin != null) {
            admin.close();
        }
    }

    @Test
    public void testSourceWithAutoCommitEnabled() throws ExecutionException, TimeoutException, InterruptedException {
        MapBasedConfig config = newCommonConfigForSource()
                .with("group.id", "test-source-with-auto-commit-enabled")
                .with(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .with(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 500)
                .with("value.deserializer", IntegerDeserializer.class.getName());
        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);

        source = new KafkaSource<>(vertx,
                "test-source-with-auto-commit-enabled",
                ic,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        Optional<IncomingKafkaRecord<String, Integer>> firstMessage = messages
                .stream()
                .map(m -> (IncomingKafkaRecord<String, Integer>) m)
                .findFirst();

        assertTrue(firstMessage.isPresent());
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        firstMessage.get().ack().whenComplete((a, t) -> ackFuture.complete(null));
        ackFuture.get(10, TimeUnit.SECONDS);

        MapBasedConfig configForAdmin = config.copy().with("client.id", "test-admin");
        admin = KafkaAdminHelper.createAdminClient(configForAdmin.getMap(), topic, true).unwrap();
        await().atMost(2, TimeUnit.MINUTES)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    Future<Map<TopicPartition, OffsetAndMetadata>> future = admin
                            .listConsumerGroupOffsets("test-source-with-auto-commit-enabled",
                                    new ListConsumerGroupOffsetsOptions()
                                            .topicPartitions(Collections.singletonList(topicPartition)))
                            .partitionsToOffsetAndMetadata();

                    Map<TopicPartition, OffsetAndMetadata> result = future.get();
                    assertNotNull(result.get(topicPartition));
                    assertEquals(10L, result.get(topicPartition).offset());
                });
    }

    @Test
    public void testSourceWithAutoCommitDisabled() throws ExecutionException, InterruptedException, TimeoutException {
        MapBasedConfig config = newCommonConfigForSource()
                .with("group.id", "test-source-with-auto-commit-disabled")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "latest");

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx, "test-source-with-auto-commit-disabled", ic,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        Message<?> last = messages.get(messages.size() - 1);
        CompletableFuture<Void> ackFuture = new CompletableFuture<>();
        last.ack().whenComplete((a, t) -> ackFuture.complete(null));
        ackFuture.get(2, TimeUnit.MINUTES);

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        MapBasedConfig configForAdmin = config.copy().with("client.id", "test-admin");
        admin = KafkaAdminHelper.createAdminClient(configForAdmin.getMap(), topic, true).unwrap();
        Future<Map<TopicPartition, OffsetAndMetadata>> future = admin
                .listConsumerGroupOffsets("test-source-with-auto-commit-disabled",
                        new ListConsumerGroupOffsetsOptions()
                                .topicPartitions(Collections.singletonList(topicPartition)))
                .partitionsToOffsetAndMetadata();

        Map<TopicPartition, OffsetAndMetadata> result = future.get();
        assertNotNull(result.get(topicPartition));
        assertEquals(10L, result.get(topicPartition).offset());
    }

    @Test
    public void testSourceWithThrottledLatestProcessedCommitEnabled() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("throttled.unprocessed-record-max-age.ms", 100);

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        MapBasedConfig configForAdmin = config.copy().with("client.id", "test-admin");
        admin = KafkaAdminHelper.createAdminClient(configForAdmin.getMap(), topic, true).unwrap();
        await().atMost(2, TimeUnit.MINUTES)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    // we must keep acking to eventually induce a commit
                    messages
                            .forEach(Message::ack);

                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    Future<Map<TopicPartition, OffsetAndMetadata>> future = admin
                            .listConsumerGroupOffsets("test-source-with-throttled-latest-processed-commit",
                                    new ListConsumerGroupOffsetsOptions()
                                            .topicPartitions(Collections.singletonList(topicPartition)))
                            .partitionsToOffsetAndMetadata();

                    Map<TopicPartition, OffsetAndMetadata> result = future.get();
                    assertNotNull(result.get(topicPartition));
                    assertEquals(10L, result.get(topicPartition).offset());
                });

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertTrue(healthReportBuilder.build().isOk());
                });
    }

    @Test
    public void testSourceWithThrottledLatestProcessedCommitEnabledWithoutAck() {
        MapBasedConfig config = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit-without-acking")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("max.poll.records", 16)
                .with("throttled.unprocessed-record-max-age.ms", 100);

        KafkaConnectorIncomingConfiguration ic = new KafkaConnectorIncomingConfiguration(config);
        source = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit-without-acking", ic,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertTrue(healthReportBuilder.build().isOk());
                });

        new Thread(() -> usage.produceIntegers(30, null,
                () -> new ProducerRecord<>(topic, counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 30);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertFalse(healthReportBuilder.build().isOk());
                });
    }

    @Test
    public void testSourceWithThrottledAndRebalance() {
        createTopic(topic, 2);
        MapBasedConfig config1 = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("throttled.unprocessed-record-max-age.ms", 1);

        MapBasedConfig config2 = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("throttled.unprocessed-record-max-age.ms", 1);

        KafkaConnectorIncomingConfiguration ic1 = new KafkaConnectorIncomingConfiguration(config1);
        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(config2);
        source = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic1,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        KafkaSource<String, Integer> source2 = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic2,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages1 = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(m -> {
            m.ack();
            messages1.add(m);
        });

        await().until(() -> source.getConsumer().getAssignments().await().indefinitely().size() == 2);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10000, null,
                () -> new ProducerRecord<>(topic, Integer.toString(counter.get() % 2), counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().subscribe().with(m -> {
            m.ack();
            messages2.add(m);
        });

        await().until(() -> source2.getConsumer().getAssignments().await().indefinitely().size() == 1
                && source.getConsumer().getAssignments().await().indefinitely().size() == 1);

        new Thread(() -> usage.produceIntegers(10000, null,
                () -> new ProducerRecord<>(topic, Integer.toString(counter.get() % 2), counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() + messages2.size() >= 10000);

        MapBasedConfig configForAdmin = config1.copy().with("client.id", "test-admin");
        admin = KafkaAdminHelper.createAdminClient(configForAdmin.getMap(), topic, true).unwrap();
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    TopicPartition tp1 = new TopicPartition(topic, 0);
                    TopicPartition tp2 = new TopicPartition(topic, 1);
                    Future<Map<TopicPartition, OffsetAndMetadata>> future = admin
                            .listConsumerGroupOffsets("test-source-with-throttled-latest-processed-commit",
                                    new ListConsumerGroupOffsetsOptions()
                                            .topicPartitions(Arrays.asList(tp1, tp2)))
                            .partitionsToOffsetAndMetadata();

                    Map<TopicPartition, OffsetAndMetadata> result = future.get();
                    assertNotNull(result.get(tp1));
                    assertNotNull(result.get(tp2));
                    assertEquals(result.get(tp1).offset() + result.get(tp2).offset(), 20000);
                });

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    HealthReport build = healthReportBuilder.build();
                    boolean ok = build.isOk();
                    if (!ok) {
                        build.getChannels().forEach(ci -> System.out.println(ci.getChannel() + " - " + ci.getMessage()));
                    }
                    assertTrue(ok);
                });

        source2.closeQuietly();
    }
}
