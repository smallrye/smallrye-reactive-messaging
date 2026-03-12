package io.smallrye.reactive.messaging.kafka.queues;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordsConverter;

public class SharedGroupConsumerTest extends KafkaCompanionTestBase {

    private String groupId;
    private String clientId;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        String methodName = testInfo.getTestMethod().get().getName();
        groupId = "group-" + methodName + "-" + UUID.randomUUID();
        clientId = "client-" + methodName + "-" + UUID.randomUUID();
        companion.consumerGroups().alterShareGroupConfig(groupId, "share.auto.offset.reset", "earliest");
    }

    private KafkaMapBasedConfig newCommonConfigForShareGroup() {
        return kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", groupId)
                .with("client.id", clientId)
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("tracing-enabled", false)
                .with("topic", topic)
                .with("graceful-shutdown", false)
                .with("share-group", true);
    }

    @Test
    public void testShareGroupSourceThroughConnector() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", "share-group-connector-test-" + topic)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("topic", topic)
                //                .with("max.poll.records", 40)
                .with("health-readiness-topic-verification", true)
                .with(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit")
                .with("share-group", true);

        addBeans(ShareGroupConsumerApp.class);
        runApplication(config, ShareGroupConsumerApp.class);

        await().until(this::isReady);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, "" + i % 2, i), 50)
                .awaitCompletion();
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, "" + i % 2, i + 50), 50)
                .awaitCompletion();
        ShareGroupConsumerApp app = get(ShareGroupConsumerApp.class);
        await().until(() -> app.received().size() >= 100);

        assertThat(app.received())
                .containsExactly(IntStream.range(0, 100).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupSourceKafkaClientService() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", groupId)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("topic", topic)
                .with("share-group", true);

        runApplication(config, ShareGroupConsumerApp.class);

        await().until(this::isReady);

        KafkaClientService service = get(KafkaClientService.class);

        // Share consumer should be accessible
        assertThat(service.getShareConsumers("data")).hasSize(1);
        assertThat(service.getShareConsumer("data")).isNotNull();
        assertThat(service.getShareConsumerChannels()).containsExactly("data");

        // Regular consumer methods should return empty/null for share group channels
        assertThat(service.getConsumers("data")).isEmpty();
        assertThat(service.getConsumer("data")).isNull();

        // Non-existent channel
        assertThat(service.getShareConsumer("missing")).isNull();
        assertThat(service.getShareConsumers("missing")).isEmpty();
    }

    @ApplicationScoped
    public static class ShareGroupConsumerApp {
        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            received.add(msg.getPayload());
            return msg.ack();
        }

        public List<Integer> received() {
            return received;
        }
    }

    @Test
    public void testShareGroupSourceThroughConnectorWithBroadcast() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", groupId)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("health-readiness-topic-verification", true)
                .with("topic", topic)
                .with("share-group", true)
                .with("broadcast", true);

        ShareGroupBroadcastApp app = runApplication(config, ShareGroupBroadcastApp.class);

        // Subscribe twice to the broadcast channel
        app.subscribe();

        await().until(this::isReady);

        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, "" + i % 2, i), 10)
                .awaitCompletion();

        // Both subscribers should receive all messages
        await().atMost(20, SECONDS).until(() -> app.getSubscriber1().size() >= 10);
        await().atMost(20, SECONDS).until(() -> app.getSubscriber2().size() >= 10);

        assertThat(app.getSubscriber1()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 10).boxed().collect(Collectors.toList()));
        assertThat(app.getSubscriber2()).containsExactlyInAnyOrderElementsOf(
                IntStream.range(0, 10).boxed().collect(Collectors.toList()));
    }

    @ApplicationScoped
    public static class ShareGroupBroadcastApp {
        private final List<Integer> subscriber1 = new CopyOnWriteArrayList<>();
        private final List<Integer> subscriber2 = new CopyOnWriteArrayList<>();

        @Inject
        @Channel("data")
        Multi<Integer> channel;

        public void subscribe() {
            channel.subscribe().with(subscriber1::add);
            channel.subscribe().with(subscriber2::add);
        }

        public Multi<Integer> getChannel() {
            return channel;
        }

        public List<Integer> getSubscriber1() {
            return subscriber1;
        }

        public List<Integer> getSubscriber2() {
            return subscriber2;
        }
    }

    @Test
    public void testGracefulShutdownFlushesAcknowledgements() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("graceful-shutdown", true)
                .with("poll-timeout", 500);

        SharedGroupConsumer app = runApplication(config, SharedGroupConsumer.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 20)
                .awaitCompletion();

        // Wait for all messages to be received and acknowledged
        await().untilAsserted(() -> assertThat(app.received()).hasSizeGreaterThanOrEqualTo(10));

        getBeanManager().createInstance()
                .select(KafkaConnector.class, ConnectorLiteral.of("smallrye-kafka")).get()
                .terminate(new Object());

        // Verify that all offsets were committed by checking share group offsets
        //        await().untilAsserted(() -> {
        //            var offsets = companion.consumerGroups().shareGroupOffsets(groupId);
        //            assertThat(offsets).containsKey(new TopicPartition(topic, 0));
        //            SharePartitionOffsetInfo info = offsets.get(KafkaCompanion.tp(topic, 0));
        //            System.out.println(info);
        //            long startOffset = info.startOffset();
        //            assertThat(startOffset).isGreaterThanOrEqualTo(10);
        //        });
    }

    @ApplicationScoped
    private static class SharedGroupConsumer {

        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking(ordered = false)
        public void consume(Integer payload) throws InterruptedException {
            received.add(payload);
            // Simulate some processing delay
            Thread.sleep(3000);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @Test
    public void testShareGroupAcknowledgementAccept() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupConsumerAccepting app = runApplication(config, ShareGroupConsumerAccepting.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // All records accepted, each delivered exactly once
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupEarliestAutoOffsetReset() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        ShareGroupConsumerAccepting app = runApplication(config, ShareGroupConsumerAccepting.class);

        // We do not need to wait for assignment

        // All records accepted, each delivered exactly once
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupAcknowledgementReject() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupConsumerRejecting app = runApplication(config, ShareGroupConsumerRejecting.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // All records rejected -- no re-delivery
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupAcknowledgementRelease() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupConsumerReleasing app = runApplication(config, ShareGroupConsumerReleasing.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Released records are re-delivered, so we should see more than 10 deliveries
        // (records with value < 3 are released on first attempt, accepted on second)
        await().atMost(20, SECONDS)
                .untilAsserted(() -> assertThat(app.accepted()).hasSizeGreaterThanOrEqualTo(10));
        assertThat(app.accepted()).containsAll(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        assertThat(app.deliveryCounts())
                .extractingByKeys(0, 1, 2)
                .allSatisfy(count -> assertThat(count).hasValue(2));
    }

    @Test
    public void testShareGroupAcknowledgementDefault() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        // Default: no explicit ack type set on ShareGroupAcknowledgement -> ACCEPT
        ShareGroupConsumerDefault app = runApplication(config, ShareGroupConsumerDefault.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupNackWithReject() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupConsumerNackRejecting app = runApplication(config, ShareGroupConsumerNackRejecting.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Nack with REJECT -- records are not re-delivered
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupNackWithRelease() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupConsumerNackReleasing app = runApplication(config, ShareGroupConsumerNackReleasing.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Records < 3 are nacked with RELEASE on first delivery, acked on re-delivery
        await().atMost(20, SECONDS).until(() -> app.acked().size() >= 10);
        assertThat(app.acked()).containsAll(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // Records 0, 1, 2 should have been delivered more than once
        assertThat(app.deliveryCounts())
                .extractingByKeys(0, 1, 2)
                .allSatisfy(count -> assertThat(count).hasValue(2));
    }

    @Test
    public void testShareGroupNackWithReleaseWithShareGroupAck() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupConsumerNackWithShareGroupAck app = runApplication(config, ShareGroupConsumerNackWithShareGroupAck.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Records < 3 are nacked with RELEASE on first delivery, acked on re-delivery
        await().atMost(20, SECONDS).until(() -> app.acked().size() >= 10);
        assertThat(app.acked()).containsAll(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // Records 0, 1, 2 should have been delivered more than once
        assertThat(app.deliveryCounts())
                .extractingByKeys(0, 1, 2)
                .allSatisfy(count -> assertThat(count).hasValue(2));
    }

    @Test
    public void testShareGroupNackDefaultReject() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());

        // Nack without metadata -- default is REJECT
        ShareGroupConsumerNackDefault app = runApplication(config, ShareGroupConsumerNackDefault.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Default nack is REJECT -- records are not re-delivered
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupBatchConsume() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("batch", true)
                .with("max.poll.records", 5)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupBatchConsumer app = runApplication(config, ShareGroupBatchConsumer.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        await().atMost(20, SECONDS).until(() -> app.received().size() >= 10);
        assertThat(app.received()).containsExactly(
                IntStream.range(0, 10).boxed().toArray(Integer[]::new));
    }

    @Test
    public void testShareGroupBatchConsumeNackRelease() {
        addBeans(ConsumerRecordsConverter.class);
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("batch", true)
                .with("max.poll.records", 5)
                .with("value.deserializer", IntegerDeserializer.class.getName());

        ShareGroupBatchNackReleaseConsumer app = runApplication(config, ShareGroupBatchNackReleaseConsumer.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 10)
                .awaitCompletion();

        // Records < 3 are nacked with RELEASE on first delivery, acked on re-delivery
        await().atMost(20, SECONDS).until(() -> app.acked().size() >= 10);
        assertThat(app.acked()).containsAll(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        // Records 0, 1, 2 should have been delivered more than once
        assertThat(app.deliveryCounts())
                .extractingByKeys(0, 1, 2)
                .allSatisfy(count -> assertThat(count).hasValue(2));
    }

    @Test
    public void testShareGroupConcurrency() {
        companion.topics().createAndWait(topic, 3);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("concurrency", 3)
                .with("max.poll.records", 5)
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("mp.messaging.incoming.data$1").with("client.id", clientId + "$1")
                .withPrefix("mp.messaging.incoming.data$2").with("client.id", clientId + "$2")
                .withPrefix("mp.messaging.incoming.data$3").with("client.id", clientId + "$3");

        ShareGroupConsumerConcurrency app = runApplication(config, ShareGroupConsumerConcurrency.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId, clientId + "$1",
                clientId + "$2", clientId + "$3")
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i % 3, "hello", i), 20)
                .awaitCompletion();

        await().atMost(20, SECONDS).until(() -> app.received().size() >= 20);
        assertThat(app.received()).containsAll(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    }

    @ApplicationScoped
    public static class ShareGroupConsumerAccepting {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer payload, ShareGroupAcknowledgement ack) {
            received.add(payload);
            ack.accept();
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerRejecting {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer payload, ShareGroupAcknowledgement ack) {
            received.add(payload);
            ack.reject();
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerReleasing {
        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Integer> accepted = new CopyOnWriteArrayList<>();
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();

        @Incoming("data")
        public void consume(Integer payload, ShareGroupAcknowledgement ack) {
            received.add(payload);
            int count = deliveryCounts.computeIfAbsent(payload, k -> new AtomicInteger(0)).incrementAndGet();
            // Release records < 3 on first delivery, accept on subsequent
            if (payload < 3 && count == 1) {
                ack.release();
            } else {
                ack.accept();
                accepted.add(payload);
            }
        }

        public List<Integer> received() {
            return received;
        }

        public List<Integer> accepted() {
            return accepted;
        }

        public Map<Integer, AtomicInteger> deliveryCounts() {
            return deliveryCounts;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerDefault {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer payload, ShareGroupAcknowledgement ack) {
            received.add(payload);
            // Don't call accept/release/reject -- default should be ACCEPT
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerNackRejecting {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            received.add(msg.getPayload());
            return msg.nack(new RuntimeException("rejected"),
                    Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.REJECT)));
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerNackReleasing {
        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Integer> acked = new CopyOnWriteArrayList<>();
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            int value = msg.getPayload();
            received.add(value);
            int count = deliveryCounts.computeIfAbsent(value, k -> new AtomicInteger(0)).incrementAndGet();
            // Release records < 3 on first delivery via nack, ack on re-delivery
            if (value < 3 && count == 1) {
                return msg.nack(new RuntimeException("releasing"),
                        Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.RELEASE)));
            } else {
                acked.add(value);
                return msg.ack();
            }
        }

        public List<Integer> received() {
            return received;
        }

        public List<Integer> acked() {
            return acked;
        }

        public Map<Integer, AtomicInteger> deliveryCounts() {
            return deliveryCounts;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerNackWithShareGroupAck {
        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Integer> acked = new CopyOnWriteArrayList<>();
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();

        @Incoming("data")
        public void consume(Integer payload, ShareGroupAcknowledgement ack) {
            received.add(payload);
            int count = deliveryCounts.computeIfAbsent(payload, k -> new AtomicInteger(0)).incrementAndGet();
            // Release records < 3 on first delivery via nack, ack on re-delivery
            if (payload < 3 && count == 1) {
                ack.release();
                throw new RuntimeException("releasing");
            } else {
                acked.add(payload);
            }
        }

        public List<Integer> received() {
            return received;
        }

        public List<Integer> acked() {
            return acked;
        }

        public Map<Integer, AtomicInteger> deliveryCounts() {
            return deliveryCounts;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerNackDefault {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            received.add(msg.getPayload());
            // Nack without metadata -- falls back to default REJECT
            return msg.nack(new RuntimeException("default nack"));
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ShareGroupBatchConsumer {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(List<Integer> payloads) {
            received.addAll(payloads);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class ShareGroupBatchNackReleaseConsumer {
        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Integer> acked = new CopyOnWriteArrayList<>();
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();

        @Incoming("data")
        public void consume(ConsumerRecords<String, Integer> records,
                IncomingKafkaRecordBatchMetadata<String, Integer> batchMetadata) {
            for (ConsumerRecord<String, Integer> record : records) {
                Integer payload = record.value();
                received.add(payload);
                int count = deliveryCounts.computeIfAbsent(payload, k -> new AtomicInteger(0)).incrementAndGet();
                // Release records < 3 on first delivery via nack, ack on re-delivery
                if (payload < 3 && count == 1) {
                    batchMetadata.getMetadataForRecord(record, ShareGroupAcknowledgement.class).release();
                } else {
                    acked.add(payload);
                }
            }
        }

        public List<Integer> received() {
            return received;
        }

        public List<Integer> acked() {
            return acked;
        }

        public Map<Integer, AtomicInteger> deliveryCounts() {
            return deliveryCounts;
        }
    }

    @Test
    public void testShareGroupDeserializationFailureRejected() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("fail-on-deserialization-failure", false);

        ShareGroupDeserializationConsumer app = runApplication(config, ShareGroupDeserializationConsumer.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        // Produce valid integers and invalid records (strings that can't be deserialized as Integer)
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i), 3)
                .awaitCompletion();
        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "key", "not-an-integer-" + i), 2)
                .awaitCompletion();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "key", i + 3), 3)
                .awaitCompletion();

        // Only valid records should reach the consumer (deserialization failures are auto-rejected)
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 6);
        assertThat(app.received()).containsExactly(0, 1, 2, 3, 4, 5);
    }

    @ApplicationScoped
    public static class ShareGroupDeserializationConsumer {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer payload) {
            received.add(payload);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @Test
    public void testShareGroupNackDefaultRelease() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName());
        // Default failure ack type is RELEASE

        ShareGroupConsumerNackAllDefault app = runApplication(config, ShareGroupConsumerNackAllDefault.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 5)
                .awaitCompletion();

        // All records are nacked without metadata -> default RELEASE -> re-delivered
        // On second delivery, they are acked
        await().atMost(20, SECONDS).until(() -> app.acked().size() >= 5);
        assertThat(app.acked()).containsAll(List.of(0, 1, 2, 3, 4));

        // Each record should have been delivered at least twice (first nack -> release, then ack)
        assertThat(app.deliveryCounts())
                .extractingByKeys(0, 1, 2, 3, 4)
                .allSatisfy(count -> assertThat(count).hasValueGreaterThanOrEqualTo(2));
    }

    @Test
    public void testShareGroupNackWithConfiguredRejectDefault() {
        companion.topics().createAndWait(topic, 1);
        KafkaMapBasedConfig config = newCommonConfigForShareGroup()
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("share-group.failure-acknowledgement-type", "reject");

        ShareGroupConsumerNackAllDefault app = runApplication(config, ShareGroupConsumerNackAllDefault.class);

        companion.consumerGroups().waitForShareGroupAssignment(groupId)
                .await().atMost(Duration.ofSeconds(20));

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "hello", i), 5)
                .awaitCompletion();

        // All records nacked without metadata -> configured default is REJECT -> not re-delivered
        await().atMost(20, SECONDS).until(() -> app.received().size() >= 5);

        // Each record should have been delivered exactly once (rejected, not re-delivered)
        assertThat(app.deliveryCounts())
                .extractingByKeys(0, 1, 2, 3, 4)
                .allSatisfy(count -> assertThat(count).hasValue(1));
    }

    @ApplicationScoped
    public static class ShareGroupConsumerNackAllDefault {
        List<Integer> received = new CopyOnWriteArrayList<>();
        List<Integer> acked = new CopyOnWriteArrayList<>();
        Map<Integer, AtomicInteger> deliveryCounts = new ConcurrentHashMap<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<Integer> msg) {
            int value = msg.getPayload();
            received.add(value);
            int count = deliveryCounts.computeIfAbsent(value, k -> new AtomicInteger(0)).incrementAndGet();
            if (count == 1) {
                // Nack without metadata on first delivery -> uses configured default
                return msg.nack(new RuntimeException("nack"));
            } else {
                acked.add(value);
                return msg.ack();
            }
        }

        public List<Integer> received() {
            return received;
        }

        public List<Integer> acked() {
            return acked;
        }

        public Map<Integer, AtomicInteger> deliveryCounts() {
            return deliveryCounts;
        }
    }

    @ApplicationScoped
    public static class ShareGroupConsumerConcurrency {
        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer payload) throws InterruptedException {
            received.add(payload);
        }

        public List<Integer> received() {
            return received;
        }
    }
}
