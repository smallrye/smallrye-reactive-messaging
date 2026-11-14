package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.providers.extension.CompositePausableChannel;

/**
 * Graceful shutdown scenarios for the Kafka connector, with its default configuration (Kafka enables graceful
 * shutdown by default): concurrency channels, share groups, and the dead-letter queue during the drain.
 * <p>
 * Each test holds in-flight messages with a gate, runs the first shutdown phase
 * ({@link ChannelLifecycleManager#prepareCloseIncoming()}) on another thread, checks that it waits for them, then
 * opens the gate and checks what the broker ends up with.
 */
public class GracefulShutdownScenariosTest extends KafkaCompanionTestBase {

    private static final int CONCURRENCY = 3;

    @Test
    void concurrencyChannelIsDrainedAsAWholeAndCommitsEveryPartition() {
        String group = "graceful-concurrency-" + UUID.randomUUID();
        companion.topics().createAndWait(topic, CONCURRENCY);

        // Remove the test-harness default (KafkaMapBasedConfig forces graceful-shutdown=false for incoming channels).
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", group)
                .with("topic", topic)
                .with("concurrency", CONCURRENCY)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("enable.auto.commit", "false")
                .with("auto.offset.reset", "earliest");
        config.remove("mp.messaging.incoming.data.graceful-shutdown");

        GatedRecordConsumer bean = runApplication(config, GatedRecordConsumer.class);

        // One consumer per indexed channel (data$1..data$3), each with a partition.
        await().untilAsserted(() -> {
            var members = companion.consumerGroups().describe(group).members();
            assertThat(members).hasSize(CONCURRENCY);
            members.forEach(member -> assertThat(member.assignment().topicPartitions()).isNotEmpty());
        });

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i % CONCURRENCY, "k-" + i, i), 9)
                .awaitCompletion(Duration.ofMinutes(1));

        // Each indexed channel consumes its partition concurrently: one record held in flight per partition.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bean.partitions()).hasSize(CONCURRENCY));

        PausableChannel pausable = get(ChannelRegistry.class).getPausable("data");
        assertThat(pausable).isInstanceOf(CompositePausableChannel.class);
        assertThat(((CompositePausableChannel) pausable).getParts()).hasSize(CONCURRENCY);

        CompletableFuture<Void> prepare = prepareCloseIncomingAsync();
        await().during(Duration.ofSeconds(1)).atMost(5, TimeUnit.SECONDS).until(() -> !prepare.isDone());

        bean.release();
        await().atMost(30, TimeUnit.SECONDS).until(prepare::isDone);

        // Get a direct reference: the bean proxy is unusable once the container is closed.
        Map<Integer, List<Integer>> processed = bean.processedByPartition();
        container.close();
        container = null;

        // Each indexed channel committed what it processed: records are processed in order per partition.
        await().untilAsserted(() -> {
            long committed = 0;
            for (int p = 0; p < CONCURRENCY; p++) {
                OffsetAndMetadata offset = companion.consumerGroups().offsets(group, new TopicPartition(topic, p));
                committed += offset == null ? 0 : offset.offset();
            }
            long processedCount = processed.values().stream().mapToInt(List::size).sum();
            assertThat(committed).isEqualTo(processedCount);
        });
    }

    @Test
    void shareGroupDrainAcknowledgesInFlightRecords() {
        String group = "graceful-share-" + UUID.randomUUID();
        int count = 20;
        companion.topics().createAndWait(topic, 1);
        companion.consumerGroups().alterShareGroupConfig(group, "share.auto.offset.reset", "earliest");

        // Remove the test-harness default (KafkaMapBasedConfig forces graceful-shutdown=false for incoming channels).
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", group)
                .with("client.id", "client-" + group)
                .with("topic", topic)
                .with("share-group", true)
                .with("tracing-enabled", false)
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("value.deserializer", IntegerDeserializer.class.getName());
        config.remove("mp.messaging.incoming.data.graceful-shutdown");

        GatedRecordConsumer bean = runApplication(config, GatedRecordConsumer.class);

        companion.consumerGroups().waitForShareGroupAssignment(group).await().atMost(Duration.ofSeconds(20));
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "k", i), count)
                .awaitCompletion();

        await().atMost(30, TimeUnit.SECONDS).until(() -> !bean.received().isEmpty());

        CompletableFuture<Void> prepare = prepareCloseIncomingAsync();
        await().during(Duration.ofSeconds(1)).atMost(5, TimeUnit.SECONDS).until(() -> !prepare.isDone());

        bean.release();
        await().atMost(30, TimeUnit.SECONDS).until(prepare::isDone);

        Set<Integer> processed = new HashSet<>(bean.received());
        container.close();
        container = null;

        // A new member of the same share group only gets the records that were not processed (and acknowledged):
        // released ones are redelivered, acknowledged ones are not.
        int remaining = count - processed.size();
        try (ConsumerTask<String, Integer> task = companion.shareConsumeIntegers()
                .withGroupId(group)
                .withOffsetReset("earliest")
                .fromTopics(topic)) {
            if (remaining > 0) {
                // Records acquired but not delivered are released on close, or after the acquisition lock expires.
                task.awaitRecords(remaining, Duration.ofSeconds(60));
            } else {
                task.awaitNoRecords(Duration.ofSeconds(5));
            }
            Set<Integer> redelivered = task.getRecords().stream().map(ConsumerRecord::value)
                    .collect(Collectors.toSet());

            assertThat(redelivered).doesNotContainAnyElementsOf(processed);
            Set<Integer> all = new HashSet<>(processed);
            all.addAll(redelivered);
            assertThat(all).containsExactlyInAnyOrderElementsOf(
                    IntStream.range(0, count).boxed().collect(Collectors.toSet()));
        }
    }

    @Test
    void recordNackedDuringTheDrainReachesTheDeadLetterQueue() {
        String group = "graceful-dlq-" + UUID.randomUUID();
        String dlqTopic = topic + "-dlq";
        companion.topics().createAndWait(topic, 1);

        // Remove the test-harness default (KafkaMapBasedConfig forces graceful-shutdown=false for incoming channels).
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", group)
                .with("topic", topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("enable.auto.commit", "false")
                .with("auto.offset.reset", "earliest")
                .with("failure-strategy", "dead-letter-queue")
                .with("dead-letter-queue.topic", dlqTopic);
        config.remove("mp.messaging.incoming.data.graceful-shutdown");

        FailingDuringDrainConsumer bean = runApplication(config, FailingDuringDrainConsumer.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 3).awaitCompletion();

        // Record 0 is held in flight: it is nacked only once the gate opens, i.e. during the drain.
        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.received().contains(0));

        CompletableFuture<Void> prepare = prepareCloseIncomingAsync();
        await().during(Duration.ofSeconds(1)).atMost(5, TimeUnit.SECONDS).until(() -> !prepare.isDone());

        bean.release();
        // The drain only completes once the nack completed, i.e. once the DLQ write was acknowledged.
        await().atMost(30, TimeUnit.SECONDS).until(prepare::isDone);

        List<Integer> received = new ArrayList<>(bean.received());
        container.close();
        container = null;

        List<Integer> dlq = companion.consumeIntegers().fromTopics(dlqTopic, 1)
                .awaitCompletion(Duration.ofSeconds(30))
                .getRecords().stream().map(ConsumerRecord::value).toList();
        assertThat(dlq).containsExactly(0);

        // The nacked record counts as processed once in the DLQ: the offset is committed past it.
        TopicPartition tp = new TopicPartition(topic, 0);
        await().untilAsserted(() -> {
            OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
            assertThat(offset).isNotNull();
            assertThat(offset.offset()).isEqualTo(received.size());
        });
    }

    private CompletableFuture<Void> prepareCloseIncomingAsync() {
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        // prepareCloseIncoming() blocks until the incoming channels are drained.
        return CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);
    }

    /**
     * Holds every record in flight until released, then acknowledges it.
     */
    @ApplicationScoped
    public static class GatedRecordConsumer {

        private final CompletableFuture<Void> gate = new CompletableFuture<>();
        private final List<Integer> received = new CopyOnWriteArrayList<>();
        private final Map<Integer, List<Integer>> byPartition = new ConcurrentHashMap<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> message) {
            received.add(message.getPayload());
            message.getMetadata(IncomingKafkaRecordMetadata.class).ifPresent(metadata -> byPartition
                    .computeIfAbsent(metadata.getPartition(), p -> new CopyOnWriteArrayList<>())
                    .add(message.getPayload()));
            return gate.thenCompose(v -> message.ack());
        }

        public void release() {
            gate.complete(null);
        }

        public List<Integer> received() {
            return received;
        }

        public Set<Integer> partitions() {
            return byPartition.keySet();
        }

        public Map<Integer, List<Integer>> processedByPartition() {
            return byPartition;
        }
    }

    /**
     * Holds record 0 until released, then nacks it; acknowledges the others.
     */
    @ApplicationScoped
    public static class FailingDuringDrainConsumer {

        private final CompletableFuture<Void> gate = new CompletableFuture<>();
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> message) {
            received.add(message.getPayload());
            if (message.getPayload() == 0) {
                return gate.thenCompose(v -> message.nack(new IllegalStateException("failed during the drain")));
            }
            return message.ack();
        }

        public void release() {
            gate.complete(null);
        }

        public List<Integer> received() {
            return received;
        }
    }
}
