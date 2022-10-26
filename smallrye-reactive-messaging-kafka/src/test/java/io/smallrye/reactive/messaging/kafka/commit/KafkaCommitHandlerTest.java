package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@SuppressWarnings("unchecked")
public class KafkaCommitHandlerTest extends KafkaCompanionTestBase {

    private KafkaSource<String, Integer> source;

    @AfterEach
    public void stopAll() {
        if (source != null) {
            source.closeQuietly();
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
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

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

        await().atMost(2, TimeUnit.MINUTES)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(
                            "test-source-with-auto-commit-enabled", topicPartition);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
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
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(), CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        Message<?> last = messages.get(messages.size() - 1);
        last.ack().toCompletableFuture().get(2, TimeUnit.MINUTES);

        await().ignoreExceptions()
                .untilAsserted(() -> {
                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(
                            "test-source-with-auto-commit-disabled", topicPartition);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
                });
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
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    // we must keep acking to eventually induce a commit
                    messages
                            .forEach(Message::ack);

                    TopicPartition topicPartition = new TopicPartition(topic, 0);
                    OffsetAndMetadata offset = companion.consumerGroups().offsets(
                            "test-source-with-throttled-latest-processed-commit", topicPartition);
                    assertNotNull(offset);
                    assertEquals(10L, offset.offset());
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
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(messages::add);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    HealthReport.HealthReportBuilder healthReportBuilder = HealthReport.builder();
                    source.isAlive(healthReportBuilder);
                    assertTrue(healthReportBuilder.build().isOk());
                });

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 30);

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
        companion.topics().createAndWait(topic, 2);
        MapBasedConfig config1 = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("throttled.unprocessed-record-max-age.ms", 100);

        MapBasedConfig config2 = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .with("throttled.unprocessed-record-max-age.ms", 100);

        KafkaConnectorIncomingConfiguration ic1 = new KafkaConnectorIncomingConfiguration(config1);
        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(config2);
        source = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic1,
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        KafkaSource<String, Integer> source2 = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic2,
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        List<Message<?>> messages1 = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(m -> {
            m.ack();
            messages1.add(m);
        });

        await().until(() -> source.getConsumer().getAssignments().await().indefinitely().size() == 2);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i % 2), i), 10000);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().subscribe().with(m -> {
            m.ack();
            messages2.add(m);
        });

        await().until(() -> source2.getConsumer().getAssignments().await().indefinitely().size() == 1
                && source.getConsumer().getAssignments().await().indefinitely().size() == 1);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i % 2), i), 10000);

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() + messages2.size() >= 10000);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> {
                    TopicPartition tp1 = new TopicPartition(topic, 0);
                    TopicPartition tp2 = new TopicPartition(topic, 1);
                    Map<TopicPartition, OffsetAndMetadata> result = companion.consumerGroups().offsets(
                            "test-source-with-throttled-latest-processed-commit", Arrays.asList(tp1, tp2));
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

    @Test
    void testSourceWithThrottledAndRebalanceWithPartitionsConfig() {
        companion.topics().createAndWait(topic, 4);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i % 2), i), 10000);

        MapBasedConfig config1 = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("partitions", 2)
                .with("commit-strategy", "throttled")
                .with(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100)
                .with("throttled.unprocessed-record-max-age.ms", 1000);

        MapBasedConfig config2 = newCommonConfigForSource()
                .with("client.id", UUID.randomUUID().toString())
                .with("group.id", "test-source-with-throttled-latest-processed-commit")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("partitions", 2)
                .with("commit-strategy", "throttled")
                .with(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100)
                .with("throttled.unprocessed-record-max-age.ms", 1000);

        KafkaConnectorIncomingConfiguration ic1 = new KafkaConnectorIncomingConfiguration(config1);
        KafkaConnectorIncomingConfiguration ic2 = new KafkaConnectorIncomingConfiguration(config2);
        source = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic1,
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        KafkaSource<String, Integer> source2 = new KafkaSource<>(vertx,
                "test-source-with-throttled-latest-processed-commit", ic2,
                commitHandlerFactories, failureHandlerFactories,
                UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents,
                UnsatisfiedInstance.instance(), -1);

        // start source1
        List<Message<?>> messages1 = Collections.synchronizedList(new ArrayList<>());
        source.getStream().subscribe().with(m -> {
            m.ack();
            messages1.add(m);
        });

        // wait for initial assignment
        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> source.getConsumer().getAssignments().await().indefinitely().size() >= 2);

        // source1 starts receiving messages
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() >= 10);

        // start source2
        List<Message<?>> messages2 = Collections.synchronizedList(new ArrayList<>());
        source2.getStream().subscribe().with(m -> {
            m.ack();
            messages2.add(m);
        });

        // wait for rebalance
        await().until(() -> {
            int sourceAssignments = source.getConsumer().getAssignments().await().indefinitely().size();
            int source2Assignments = source2.getConsumer().getAssignments().await().indefinitely().size();
            return sourceAssignments >= 1 && source2Assignments >= 1 && sourceAssignments + source2Assignments == 4;
        });

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i % 2), i), 10000);

        // source 2 starts receiving messages
        await().atMost(2, TimeUnit.MINUTES).until(() -> messages2.size() >= 4000);

        Set<TopicPartition> source2Partitions = source2.getConsumer().getAssignments().await().indefinitely();
        await().untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> offsets = companion.consumerGroups().offsets(
                    "test-source-with-throttled-latest-processed-commit", new ArrayList<>(source2Partitions));
            assertThat(offsets).isNotNull();
        });
        // quit source2
        source2.closeQuietly();

        await().atMost(2, TimeUnit.MINUTES).until(() -> messages1.size() + messages2.size() >= 20000);

    }
}
