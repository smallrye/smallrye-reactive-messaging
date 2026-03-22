package io.smallrye.reactive.messaging.kafka.queues;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;
import io.smallrye.reactive.messaging.kafka.impl.KafkaShareGroupSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaShareConsumer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class KafkaShareGroupSourceTest extends KafkaCompanionTestBase {

    KafkaShareGroupSource<String, Integer> source;
    KafkaConnector connector;
    private String groupId;
    private String clientId;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        String methodName = testInfo.getTestMethod().get().getName();
        groupId = "group-" + methodName + "-" + UUID.randomUUID();
        clientId = "client-" + methodName + "-" + UUID.randomUUID();
        companion.consumerGroups().alterShareGroupConfig(groupId, "share.auto.offset.reset", "earliest");
    }

    @AfterEach
    public void closing() {
        if (source != null) {
            source.closeQuietly();
            source = null;
        }
        if (connector != null) {
            connector.terminate(new Object());
            connector = null;
        }
    }

    private MapBasedConfig newCommonConfigForShareGroup() {
        return kafkaConfig().build(
                "group.id", groupId,
                "client.id", clientId,
                "share.auto.offset.reset", "earliest", // Doesn't seem to be working, but set it just in case
                "key.deserializer", StringDeserializer.class.getName(),
                "tracing-enabled", false,
                "topic", topic,
                "poll-timeout", 100,
                "graceful-shutdown", false,
                "share-group", true,
                "channel-name", topic);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testShareGroupSource() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        source = createShareGroupSource(groupId, config);

        List<Message<?>> messages = new CopyOnWriteArrayList<>();
        source.getStream().subscribe().with(messages::add);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        await().atMost(20, SECONDS).until(() -> messages.size() >= 10);
        assertThat(messages.stream().map(m -> ((KafkaRecord<String, Integer>) m).getPayload())
                .collect(Collectors.toList()))
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testShareGroupSourceWithAcknowledgment() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        source = createShareGroupSource(groupId, config);

        List<Integer> acknowledged = new CopyOnWriteArrayList<>();
        source.getStream().call(record -> {
            acknowledged.add(record.getPayload());
            return Uni.createFrom().completionStage(record.ack());
        }).subscribe().with(msg -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        await().atMost(20, SECONDS).until(() -> acknowledged.size() >= 10);
        assertThat(acknowledged).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testShareGroupSourceWithNack() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        source = createShareGroupSource(groupId, config);

        AtomicInteger processedCount = new AtomicInteger(0);
        List<Integer> processed = new CopyOnWriteArrayList<>();

        source.getStream().call(record -> {
            int value = record.getPayload();
            processed.add(value);

            // Nack messages with value < 5 to test rejection
            if (value < 5) {
                return Uni.createFrom().completionStage(
                        record.nack(new RuntimeException("Rejecting message " + value),
                                Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.REJECT))));
            } else {
                processedCount.incrementAndGet();
                return Uni.createFrom().completionStage(record.ack());
            }
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        // Verify we tried to process all messages
        await().atMost(20, SECONDS)
                .untilAsserted(() -> assertThat(processed).hasSizeGreaterThanOrEqualTo(10));

        // Should successfully process messages >= 5
        await().atMost(20, SECONDS)
                .untilAsserted(() -> assertThat(processedCount).hasValueGreaterThanOrEqualTo(5));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testShareGroupSourceWithBatch() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("batch", true);
        source = createShareGroupSource(groupId, config);

        List<Message<?>> batches = new CopyOnWriteArrayList<>();
        source.getBatchStream().subscribe().with(batches::add);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        await().atMost(20, SECONDS).until(() -> {
            int totalRecords = batches.stream()
                    .mapToInt(m -> ((KafkaRecordBatch<String, Integer>) m).getRecords().size())
                    .sum();
            return totalRecords >= 10;
        });

        // Verify we received all records across batches
        List<Integer> allValues = batches.stream()
                .flatMap(m -> ((KafkaRecordBatch<String, Integer>) m).getRecords().stream())
                .map(KafkaRecord::getPayload)
                .collect(Collectors.toList());
        assertThat(allValues).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testShareGroupSourceThroughKafkaConsumer() {
        companion.topics().createAndWait(topic, 1);

        Properties props = new Properties();
        props.put("bootstrap.servers", companion.getBootstrapServers());
        props.put("group.id", groupId);
        props.put("client.id", clientId);
        props.put("share.acknowledgement.mode", "explicit");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", IntegerDeserializer.class.getName());

        // Create the Kafka share consumer
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaShareConsumer<String, Integer>(props)) {
            // Subscribe to topics
            consumer.subscribe(List.of(topic));
            var ackedOffsets = new CopyOnWriteArrayList<Long>();
            consumer.setAcknowledgementCommitCallback((offsets, exception) -> {
                ackedOffsets.addAll(offsets.values().stream().flatMap(Collection::stream).toList());
            });

            await().until(() -> !consumer.subscription().isEmpty());
            consumer.poll(Duration.ofMillis(100));

            companion.consumerGroups().waitForShareGroupAssignment(groupId)
                    .await().atMost(Duration.ofSeconds(20));

            companion.produceIntegers()
                    .usingGenerator(i -> new ProducerRecord<>(topic, "" + i % 5, i), 100)
                    .awaitCompletion();

            var payloads = new CopyOnWriteArrayList<Integer>();
            // Poll for records
            while (payloads.size() < 10) {
                var records = consumer.poll(Duration.ofMillis(3000));
                for (var record : records) {
                    payloads.add(record.value());
                    consumer.acknowledge(record);
                }
                consumer.commitSync();
            }
            assertThat(payloads).hasSizeGreaterThanOrEqualTo(10);
            await().atMost(20, SECONDS).untilAsserted(() -> assertThat(ackedOffsets).hasSizeGreaterThanOrEqualTo(10));
        }
    }

    @Test
    public void testShareGroupWithMultipleConsumers() {
        companion.topics().createAndWait(topic, 2);
        MapBasedConfig config = new MapBasedConfig()
                .with("bootstrap.servers", companion.getBootstrapServers())
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("graceful-shutdown", false)
                .with("topic", topic)
                .with("share.auto.offset.reset", "earliest")
                .with("share-group", true)
                .with("channel-name", "data");

        Map<TopicPartition, Set<Long>> ackedOffsets = new ConcurrentHashMap<>();

        // Create two share group sources with the same group ID
        KafkaShareGroupSource<String, Integer> source1 = createShareGroupSource(groupId, config
                .with("client.id", clientId + "$1"));
        source1.getConsumer().runOnPollingThread(c -> {
            c.setAcknowledgementCommitCallback((offsets, exception) -> {
                offsets.forEach((partition, offset) -> ackedOffsets.put(partition.topicPartition(), offset));
            });
        }).await().indefinitely();

        KafkaShareGroupSource<String, Integer> source2 = createShareGroupSource(groupId, config
                .with("client.id", clientId + "$2"));
        source2.getConsumer().runOnPollingThread(c -> {
            c.setAcknowledgementCommitCallback((offsets, exception) -> {
                offsets.forEach((partition, offset) -> ackedOffsets.put(partition.topicPartition(), offset));
            });
        }).await().indefinitely();

        List<Integer> consumer1Messages = new CopyOnWriteArrayList<>();
        List<Integer> consumer2Messages = new CopyOnWriteArrayList<>();

        source1.getStream().call(r -> {
            consumer1Messages.add(r.getPayload());
            return Uni.createFrom().completionStage(r.ack());
        }).subscribe().with(record -> {
        });

        source2.getStream().call(r -> {
            consumer2Messages.add(r.getPayload());
            return Uni.createFrom().completionStage(r.ack());
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId,
                source1.getConsumer().getClientId(), source2.getConsumer().getClientId())
                .await().atMost(Duration.ofSeconds(20));

        // Produce 100 messages
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i % 2, "" + i % 2, i), 100)
                .awaitCompletion();

        // Wait for all messages to be consumed
        await().atMost(20, SECONDS).untilAsserted(
                () -> assertThat(consumer1Messages.size() + consumer2Messages.size()).isGreaterThanOrEqualTo(100));

        // Share group distributes load, but distribution is not guaranteed —
        // one consumer may receive all records. Only check combined total.

        // Combined they should have all messages
        List<Integer> allMessages = new ArrayList<>();
        allMessages.addAll(consumer1Messages);
        allMessages.addAll(consumer2Messages);
        assertThat(allMessages).hasSize(100).containsExactlyInAnyOrderElementsOf(
                java.util.stream.IntStream.range(0, 100).boxed().collect(Collectors.toList()));

        await().untilAsserted(() -> {
            assertThat(ackedOffsets).hasSize(2);
            assertThat(ackedOffsets.get(KafkaCompanion.tp(topic, 0)))
                    .containsExactlyInAnyOrder(LongStream.range(0, 50).boxed().toArray(Long[]::new));
            assertThat(ackedOffsets.get(KafkaCompanion.tp(topic, 1)))
                    .containsExactlyInAnyOrder(LongStream.range(0, 50).boxed().toArray(Long[]::new));
        });

        // Clean up
        source1.closeQuietly();
        source2.closeQuietly();
    }

    @Test
    public void testShareGroupSourceWithBatchAcknowledgment() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("batch", true);
        source = createShareGroupSource(groupId, config);

        List<Integer> acknowledged = new CopyOnWriteArrayList<>();
        source.getBatchStream().call(batch -> {
            for (KafkaRecord<String, Integer> record : batch.getRecords()) {
                acknowledged.add(record.getPayload());
            }
            return Uni.createFrom().completionStage(batch.ack());
        }).subscribe().with(batch -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        // All 10 records should be acknowledged, not just the latest per partition
        await().atMost(20, SECONDS).until(() -> acknowledged.size() >= 10);
        assertThat(acknowledged).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testShareGroupSourceWithBatchNack() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("batch", true);
        source = createShareGroupSource(groupId, config);

        List<Integer> processed = new CopyOnWriteArrayList<>();
        source.getBatchStream().call(batch -> {
            for (KafkaRecord<String, Integer> record : batch.getRecords()) {
                processed.add(record.getPayload());
            }
            // Nack the entire batch with REJECT
            return Uni.createFrom().completionStage(
                    batch.nack(new RuntimeException("batch failure"),
                            Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.REJECT))));
        }).subscribe().with(batch -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        // All records in the batch should be nacked (rejected), not just the latest per partition
        await().atMost(20, SECONDS).until(() -> processed.size() >= 10);
    }

    @Test
    public void testShareGroupSourceNackWithRelease() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        source = createShareGroupSource(groupId, config);

        // Track how many times each value is seen
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();
        List<Integer> acked = new CopyOnWriteArrayList<>();

        source.getStream().call(record -> {
            int value = record.getPayload();
            int count = deliveryCounts.computeIfAbsent(value, k -> new AtomicInteger(0)).incrementAndGet();

            // Release messages with value < 3 on first delivery (they should be re-delivered)
            if (value < 3 && count == 1) {
                return Uni.createFrom().completionStage(
                        record.nack(new RuntimeException("Releasing message " + value),
                                Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.RELEASE))));
            } else {
                acked.add(value);
                return Uni.createFrom().completionStage(record.ack());
            }
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        // All 10 unique values should eventually be acked (including re-delivered ones)
        await().atMost(20, SECONDS).until(() -> acked.size() >= 10);

        // Released values (0, 1, 2) should have been delivered more than once
        await().atMost(20, SECONDS).untilAsserted(() -> {
            for (int i = 0; i < 3; i++) {
                assertThat(deliveryCounts.get(i).get()).isGreaterThan(1);
            }
        });
    }

    @Test
    @Disabled
    public void testShareGroupSourceNackWithAccept() {
        companion.topics().createAndWait(topic, 1);

        source = createShareGroupSource(groupId, newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName()));

        List<Integer> processed = new CopyOnWriteArrayList<>();

        source.getStream().call(record -> {
            int value = record.getPayload();
            processed.add(value);

            // Nack message with value == 5 using ACCEPT (treated as success, no re-delivery)
            if (value == 5) {
                return Uni.createFrom().completionStage(
                        record.nack(new RuntimeException("Accept-nacking message " + value),
                                Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.ACCEPT))));
            } else {
                return Uni.createFrom().completionStage(record.ack());
            }
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        await().atMost(30, SECONDS).untilAsserted(() -> assertThat(processed).hasSizeGreaterThanOrEqualTo(10));

        // ACCEPT means no re-delivery, so each value should appear exactly once
        assertThat(processed).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void testShareGroupSourceNackWithDefaultReject() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        source = createShareGroupSource(groupId, config);

        AtomicInteger ackedCount = new AtomicInteger(0);
        List<Integer> processed = new CopyOnWriteArrayList<>();

        source.getStream().call(record -> {
            int value = record.getPayload();
            processed.add(value);

            // Nack messages with value < 5 without providing any metadata
            // The handler should fall back to the default AcknowledgeType (REJECT)
            if (value < 5) {
                return Uni.createFrom().completionStage(record.nack(new RuntimeException("Rejecting " + value)));
            } else {
                ackedCount.incrementAndGet();
                return Uni.createFrom().completionStage(record.ack());
            }
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10);

        // Messages >= 5 should be successfully acked
        await().atMost(20, SECONDS).until(() -> ackedCount.get() >= 5);

        // All messages should have been processed (rejected ones are not re-delivered)
        await().atMost(20, SECONDS).until(() -> processed.size() >= 10);
    }

    @Test
    public void testShareGroupSourceGracefulShutdown() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("graceful-shutdown", true);
        source = createShareGroupSource(groupId, config);
        ReactiveKafkaShareConsumer<String, Integer> consumer = source.getConsumer();

        List<Integer> received = new CopyOnWriteArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        source.getStream().call(record -> {
            received.add(record.getPayload());
            return Uni.createFrom().completionStage(record.ack());
        }).subscribe().with(
                record -> {
                },
                failure -> {
                },
                () -> completed.set(true));

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 20);

        // Wait for some messages to arrive
        await().atMost(20, SECONDS).until(() -> received.size() >= 10);

        // Graceful shutdown should not throw
        source.closeQuietly();
        source = null;

        assertThatThrownBy(() -> consumer.unwrap().poll(Duration.ofMillis(1000)))
                .isInstanceOf(IllegalStateException.class)
                .message().isEqualTo("This consumer has already been closed.");

    }

    @Test
    public void testShareGroupSourceNoRenewWhenTimeoutDisabled() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("graceful-shutdown", true)
                .with("poll-timeout", 500)
                .with("share-group.unprocessed-record-max-age.ms", 0);
        source = createShareGroupSource(groupId, config);

        // Track delivery counts per value to detect re-deliveries
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();
        List<Integer> acked = new CopyOnWriteArrayList<>();

        source.getStream().call(record -> {
            int value = record.getPayload();
            deliveryCounts.computeIfAbsent(value, k -> new AtomicInteger(0)).incrementAndGet();
            // Simulate slow processing -- no RENEW is sent when timeout is disabled,
            // so if processing exceeds the broker's acquisition lock duration (default 30s),
            // the record would be re-delivered. Here we stay well within the lock duration.
            return Uni.createFrom().item(record)
                    .onItem().delayIt().by(Duration.ofSeconds(1))
                    .onItem().transformToUni(r -> {
                        acked.add(value);
                        return Uni.createFrom().completionStage(r.ack());
                    });
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 5)
                .awaitCompletion();

        // All 5 unique values should eventually be acked
        await().atMost(20, SECONDS).until(() -> acked.size() >= 5);
        assertThat(acked).containsAll(List.of(0, 1, 2, 3, 4));

        // No timeout failure should be reported (liveness stays OK)
        var builder = io.smallrye.reactive.messaging.health.HealthReport.builder();
        source.isAlive(builder);
        assertThat(builder.build().isOk()).isTrue();

        // Graceful shutdown should still work
        source.closeQuietly();
        source = null;
    }

    @Test
    public void testShareGroupSourceProcessingTimeout() {
        companion.topics().createAndWait(topic, 1);
        MapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("poll-timeout", 500)
                .with("share-group.unprocessed-record-max-age.ms", 2000);
        source = createShareGroupSource(groupId, config);

        List<Integer> processed = new CopyOnWriteArrayList<>();

        source.getStream().call(record -> {
            int value = record.getPayload();
            processed.add(value);

            if (value == 5) {
                // Deliberately delay processing to exceed the 2s timeout
                return Uni.createFrom().item(record)
                        .onItem().delayIt().by(Duration.ofSeconds(5))
                        .onItem().transformToUni(r -> Uni.createFrom().completionStage(r.ack()));
            } else {
                return Uni.createFrom().completionStage(record.ack());
            }
        }).subscribe().with(record -> {
        });

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Wait for the processing timeout to trigger a failure report
        // The commit handler should detect record 5 exceeded the 2s timeout
        // and report a failure via reportFailure
        await().until(() -> processed.size() >= 10);

        // Verify that the source detected the timeout (reported as a failure, visible in liveness)
        await().untilAsserted(() -> {
            var builder = io.smallrye.reactive.messaging.health.HealthReport.builder();
            source.isAlive(builder);
            var report = builder.build();
            assertThat(report.isOk()).isFalse();
        });
    }

}
