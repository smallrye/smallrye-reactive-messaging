package io.smallrye.reactive.messaging.kafka.client;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.reactive.messaging.kafka.CountKafkaCdiEvents;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.smallrye.reactive.messaging.kafka.base.KafkaBrokerExtension;
import io.smallrye.reactive.messaging.kafka.base.SingletonInstance;
import io.smallrye.reactive.messaging.kafka.base.TopicHelpers;
import io.smallrye.reactive.messaging.kafka.base.UnsatisfiedInstance;
import io.smallrye.reactive.messaging.kafka.impl.KafkaSource;
import io.smallrye.reactive.messaging.kafka.impl.ReactiveKafkaConsumer;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.strimzi.StrimziKafkaContainer;

public class ReactiveKafkaConsumerTest extends ClientTestBase {

    private final Semaphore assignSemaphore = new Semaphore(partitions);
    private final List<Cancellable> subscriptions = new ArrayList<>();
    private KafkaSource<Integer, String> source;

    @AfterEach
    public void tearDown() {
        cancelSubscriptions();
        source.closeQuietly();
    }

    @BeforeEach
    public void init() {
        topic = TopicHelpers.createNewTopic("test-" + UUID.randomUUID().toString(), partitions);
        resetMessages();
    }

    @Test
    public void testReception() throws Exception {
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource().getStream();
        sendReceive(stream, 0, 100, 0, 100);
    }

    @Test
    public void testRequests() throws Exception {
        sendMessages(0, 10);
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource().getStream();

        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                .onItem().invoke(item -> CompletableFuture.runAsync(item::ack))
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        waitFoPartitionAssignment();

        await().untilAsserted(() -> subscriber.assertSubscribed().assertHasNotReceivedAnyItem());

        subscriber.request(2);
        await().until(() -> subscriber.getItems().size() == 2);
        await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());

        subscriber.request(3);
        await().until(() -> subscriber.getItems().size() == 5);
        await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());

        subscriber.request(50);
        await().until(() -> subscriber.getItems().size() == 10);
        await().until(() -> source.getConsumer().paused().await().indefinitely().isEmpty());

        sendMessages(10, 45);
        await().until(() -> subscriber.getItems().size() == 55);
        await().until(() -> source.getConsumer().paused().await().indefinitely().isEmpty());

        waitForCommits(source, 55);
    }

    @Test
    public void testUnboundedRequests() throws Exception {
        sendMessages(0, 10);
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource().getStream();

        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                .onItem().invoke(item -> CompletableFuture.runAsync(item::ack))
                .subscribe().withSubscriber(AssertSubscriber.create());

        waitFoPartitionAssignment();
        await()
                .untilAsserted(() -> subscriber.assertSubscribed().assertHasNotReceivedAnyItem());

        subscriber.request(Long.MAX_VALUE);

        await().until(() -> subscriber.getItems().size() == 10);
        await().until(() -> source.getConsumer().paused().await().indefinitely().isEmpty());

        sendMessages(10, 45);
        await().until(() -> subscriber.getItems().size() == 55);
        await().until(() -> source.getConsumer().paused().await().indefinitely().isEmpty());

        waitForCommits(source, 55);
    }

    @Test
    public void testNoPauseIfRequestsButNoMessage() throws Exception {
        sendMessages(0, 10);
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource().getStream();

        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                .onItem().invoke(item -> CompletableFuture.runAsync(item::ack))
                .subscribe().withSubscriber(AssertSubscriber.create(11));

        waitFoPartitionAssignment();

        await().untilAsserted(subscriber::assertSubscribed);
        await().until(() -> subscriber.getItems().size() == 10);

        await()
                .pollDelay(Duration.ofMillis(500))
                .until(() -> subscriber.getItems().size() == 10);

        waitForCommits(source, 10);
        assertThat(source.getConsumer().paused().await().indefinitely()).isEmpty();
    }

    @Test
    public void testReceptionWithHeaders() throws Exception {
        int count = 10;
        int partition = 0;
        List<IncomingKafkaRecord<Integer, String>> received = new ArrayList<>();
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource().getStream()
                .invoke(received::add);
        CountDownLatch latch = new CountDownLatch(count);
        subscribe(stream, latch);
        List<ProducerRecord<Integer, String>> sent = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            @SuppressWarnings("UnnecessaryLocalVariable")
            int key = i;
            String value = String.valueOf(i);
            long timestamp = System.currentTimeMillis();
            ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, partition, timestamp, key, value);
            record.headers().add("header1", new byte[] { (byte) 0 });
            record.headers().add("header2", value.getBytes());
            sent.add(record);
        }
        sendMessages(sent.stream());
        waitForMessages(latch);
        assertThat(count).isEqualTo(received.size());

        for (int i = 0; i < count; i++) {
            ProducerRecord<Integer, String> sRec = sent.get(i);
            IncomingKafkaRecord<Integer, String> rRec = received.get(i);
            assertThat(sRec.key()).isEqualTo(rRec.getKey());
            assertThat(sRec.value()).isEqualTo(rRec.getPayload());
            assertThat(sRec.topic()).isEqualTo(rRec.getTopic());
            assertThat(sRec.partition()).isEqualTo(rRec.getPartition());
            assertThat(sRec.timestamp() / 1000).isEqualTo(rRec.getTimestamp().getEpochSecond());
            assertThat(rRec.getHeaders()).hasSize(2);
            assertThat(rRec.getHeaders()).isEqualTo(sRec.headers());
        }
    }

    @Test
    public void testRebalanceListenerSeekToBeginning() throws Exception {
        int count = 10;
        sendMessages(0, count);

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSourceSeekToBeginning().getStream();
        sendReceive(stream, count, count, 0, count * 2);
    }

    @Test
    public void testRebalanceListenerSeekToEnd() throws Exception {
        int count = 10;
        sendMessages(0, count);
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSourceSeekToEnd().getStream();
        sendReceiveWithSendDelay(stream, Duration.ofMillis(100), count, count);
    }

    @Test
    public void testRebalanceListenerSeekToOffset() throws Exception {
        int count = 10;
        sendMessages(0, count);
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSourceSeekToOffset().getStream();
        sendReceive(stream, count, count, partitions, count * 2 - partitions);
    }

    @Test
    public void testOffsetResetLatest() throws Exception {
        int count = 10;
        sendMessages(0, count);

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignation());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);

        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = source.getStream()
                .invoke(this::onReceive)
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        await().until(() -> {
            Map<TopicPartition, Long> map = source.getConsumer().getPositions().await().indefinitely();
            return map.values().stream().mapToLong(l -> l).sum() == 10;
        });

        subscriber
                .assertSubscribed()
                .assertHasNotReceivedAnyItem();

        sendMessages(count, count);

        await()
                .untilAsserted(() -> assertThat(subscriber.getItems()).hasSize(count));

        await().until(() -> {
            Map<TopicPartition, Long> map = source.getConsumer().getPositions().await().indefinitely();
            return map.values().stream().mapToLong(l -> l).sum() == 20;
        });

        subscriber.cancel();

        checkConsumedMessages(count, count);
    }

    @Test
    public void testSubscriptionUsingWildcard() throws Exception {
        String prefix = UUID.randomUUID().toString();
        topic = createNewTopicWithPrefix(prefix);

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", prefix + ".*")
                .with("pattern", true);

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream();
        sendReceive(stream, 0, 10, 0, 10);
    }

    @Test
    public void testAcknowledgementUsingThrottledStrategy() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic);

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(IncomingKafkaRecord::ack);

        sendReceive(stream, 0, 100, 0, 100);
        waitForCommits(source, 100);

        // Close consumer and create another one. First consumer should commit final offset on close.
        // Second consumer should receive only new messages.
        cancelSubscriptions();
        clearReceivedMessages();
        config.with(ConsumerConfig.CLIENT_ID_CONFIG, "second-consumer");
        Multi<IncomingKafkaRecord<Integer, String>> stream2 = createSource(config, groupId).getStream();
        sendReceive(stream2, 100, 100, 100, 100);
    }

    @Test
    public void testAcknowledgementUsingThrottledStrategyEvenAfterBrokerRestart() throws Exception {
        try (StrimziKafkaContainer kafka = new StrimziKafkaContainer(KafkaBrokerExtension.KAFKA_VERSION)) {
            kafka.start();
            await().until(kafka::isRunning);

            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with("auto.commit.interval.ms", 10);

            Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                    .invoke(IncomingKafkaRecord::ack);

            CountDownLatch latch = new CountDownLatch(100);
            subscribe(stream, latch);
            try (final FixedKafkaContainer ignored = restart(kafka, 3)) {
                sendMessages(0, 100);
                waitForMessages(latch);
                checkConsumedMessages(0, 100);
                waitForCommits(source, 100);
            }
        }
    }

    @Test
    public void testThrottledAcknowledgementWithDefaultValues() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(IncomingKafkaRecord::ack);

        sendReceive(stream, 0, 100, 0, 100);
        restartAndCheck(config, groupId, 1);
    }

    @Test
    public void testThrottledAcknowledgementWithIntervalSet() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic)
                .with(AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(IncomingKafkaRecord::ack);

        sendReceive(stream, 0, 100, 0, 100);
        Thread.sleep(1500);
        restartAndCheck(config, groupId, 0);
    }

    @Test
    public void testAcknowledgementWhilePaused() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        List<IncomingKafkaRecord<Integer, String>> list = new ArrayList<>();
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(list::add);

        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        waitFoPartitionAssignment();

        sendMessages(0, 100);
        subscriber.request(50);
        await().until(() -> list.size() == 50);
        await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());

        list.forEach(IncomingKafkaRecord::ack);
        list.clear();

        waitForCommits(source, 50);

        subscriber.request(50);
        await().until(() -> list.size() == 50);
        list.forEach(IncomingKafkaRecord::ack);
        waitForCommits(source, 50);
    }

    @Test
    public void testLatestAcknowledgement() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic)
                .with("commit-strategy", "latest");

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(IncomingKafkaRecord::ack);

        sendReceive(stream, 0, 100, 0, 100);
        restartAndCheck(config, groupId, 1);
    }

    @Test
    public void testAutoCommitAcknowledgement() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic)
                .with(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(IncomingKafkaRecord::ack);

        sendReceive(stream, 0, 100, 0, 100);
        restartAndCheck(config, groupId, 1);
    }

    @RepeatedTest(10)
    public void testRebalanceWhilePausedAndPendingCommit() throws Exception {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);
        MapBasedConfig config2 = createConsumerConfig(groupId)
                .with(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + groupId + "-2")
                .with("topic", topic);

        List<IncomingKafkaRecord<Integer, String>> list = new CopyOnWriteArrayList<>();
        List<IncomingKafkaRecord<Integer, String>> list2 = new CopyOnWriteArrayList<>();

        // The first source do not commit for now.
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .invoke(list::add);
        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                .subscribe().withSubscriber(AssertSubscriber.create(0));
        waitFoPartitionAssignment();

        sendMessages(0, 100);

        // Request 50 messages and wait for them to be received. They are not acknowledged
        subscriber.request(50);
        await().until(() -> list.size() == 50);

        // Create the second source acknowledging the message.
        // The rebalance will split the partitions between the 2 sources, but both will restaert from offset 0, as nothing
        // has been acked.
        KafkaSource<Integer, String> source2 = new KafkaSource<>(vertx, groupId,
                new KafkaConnectorIncomingConfiguration(config2), UnsatisfiedInstance.instance(),
                CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 3);
        source2.getStream()
                .invoke(i -> {
                    list2.add(i);
                    i.ack();
                }).subscribe().withSubscriber(AssertSubscriber.create(100));
        await().until(() -> !source2.getConsumer().getAssignments().await().indefinitely().isEmpty());

        // Verify rebalance
        await().until(() -> Uni.combine().all()
                .unis(source.getConsumer().getAssignments(), source2.getConsumer().getAssignments())
                .combinedWith((tp1, tp2) -> tp1.size() + tp2.size()).await().indefinitely() == partitions);
        Set<TopicPartition> assignedToSource2 = source2.getConsumer().getAssignments().await().indefinitely();

        subscriber.request(100);
        await().until(() -> list.size() >= 100);
        await().until(() -> list2.size() == assignedToSource2.size() * 25);

        // Acknowledge messages, even these received before the rebalance.
        list.forEach(IncomingKafkaRecord::ack);

        // Verify that the 100 messages have been received.
        await().untilAsserted(() -> {
            List<String> receivedByFirstSource = list.stream().map(i -> i.getPartition() + "/" + i.getOffset())
                    .collect(Collectors.toList());
            List<String> receivedBySecondSource = list2.stream().map(i -> i.getPartition() + "/" + i.getOffset())
                    .collect(Collectors.toList());

            Set<String> set = new HashSet<>(receivedByFirstSource);
            set.addAll(receivedBySecondSource);
            assertThat(set).hasSize(100);
        });

        source2.closeQuietly();
    }

    @Test
    public void testCommitWithLatestStrategy() throws Exception {
        int count = 10;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        Arrays.fill(committedOffsets, 0L);

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic)
                .with("commit-strategy", "latest");

        // Acknowledge messages immediately
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .onItem().call(x -> {
                    assertThat(committedOffsets[x.getPartition()]).isEqualTo(x.getOffset());
                    return Uni.createFrom().completionStage(x.ack())
                            .onItem().invoke(v -> onCommit(x, commitLatch, committedOffsets));
                });

        sendAndWaitForMessages(stream, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    @Test
    public void testCommitBatchWithLatestStrategy() throws Exception {
        int count = 20;
        int commitIntervalMessages = 4;
        CountDownLatch commitLatch = new CountDownLatch(count);
        long[] committedOffsets = new long[partitions];
        Arrays.fill(committedOffsets, 0L);

        List<IncomingKafkaRecord<?, ?>> uncommitted = new ArrayList<>();
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .with("topic", topic)
                .with("commit-strategy", "latest");

        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream()
                .onItem().call(record -> {
                    uncommitted.add(record);
                    if (uncommitted.size() == commitIntervalMessages) {
                        return Uni.createFrom().completionStage(record.ack())
                                .invoke(i -> onCommit(uncommitted, commitLatch, committedOffsets));
                    }
                    return Uni.createFrom().voidItem();
                });

        sendAndWaitForMessages(stream, count);
        checkCommitCallbacks(commitLatch, committedOffsets);
    }

    /**
     * Tests that delays in message processing dont cause session timeouts.
     * Kafka consumer heartbeat thread should keep the session alive.
     */
    @Test
    public void testMessageProcessingDelay() throws Exception {
        int count = 3;

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        KafkaSource<Integer, String> source = createSource(config, groupId);

        AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = source.getStream()
                .select().first(count)
                .subscribe().withSubscriber(AssertSubscriber.create(0));

        sendMessages(0, count);

        for (int i = 0; i < count; i++) {
            subscriber.request(1);
            int l = i + 1;
            await().until(() -> subscriber.getItems().size() == l);
            Thread.sleep(sessionTimeoutMillis + 1000);
        }

        subscriber.awaitCompletion();
    }

    @Test
    public void testWithBrokerRestart() throws Exception {
        int sendBatchSize = 10;
        try (StrimziKafkaContainer kafka = new StrimziKafkaContainer(KafkaBrokerExtension.KAFKA_VERSION)) {
            kafka.start();
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .put("topic", topic);

            KafkaSource<Integer, String> source = createSource(config, groupId);
            CountDownLatch receiveLatch = new CountDownLatch(sendBatchSize * 2);
            subscribe(source.getStream(), receiveLatch);
            sendMessages(0, sendBatchSize);
            try (FixedKafkaContainer ignored = restart(kafka, 5)) {
                sendMessages(sendBatchSize, sendBatchSize);
                waitForMessages(receiveLatch);
                checkConsumedMessages();
            }
        }
    }

    @Test
    public void testThatCloseReleaseTheAssignments() throws Exception {
        int count = 10;
        for (int i = 0; i < 2; i++) {
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .put("topic", topic);
            KafkaSource<Integer, String> source = createSource(config, groupId);

            sendAndWaitForMessages(source.getStream()
                    .onItem().invoke(IncomingKafkaRecord::ack), count);
            if (i == 0) {
                waitForCommits(source, count);
            }

            await().until(() -> !source.getConsumer().getAssignments().await().indefinitely().isEmpty());
            assertThat(source.getConsumer().getAssignments().await().indefinitely()).hasSize(partitions);

            source.closeQuietly();
        }
    }

    @Test
    public void testWithMultipleConsumersWithASingleConsumeGroup() throws Exception {
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count);

        String groupId = UUID.randomUUID().toString();

        List<KafkaSource<Integer, String>> sources = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + i)
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaSource<Integer, String> source = createSource(config, groupId);
            sources.add(source);
            source.getStream()
                    .invoke(rec -> {
                        onReceive(rec);
                        latch.countDown();
                    })
                    .subscribe().with(IncomingKafkaRecord::ack);
            await().until(() -> !source.getConsumer().getAssignments().await().indefinitely().isEmpty());
        }
        sendMessages(0, count);
        waitForMessages(latch);
        checkConsumedMessages(0, count);

        assertThat(sources).hasSize(4);
        Set<TopicPartition> sets = sources.stream()
                .map(s -> s.getConsumer().getAssignments().await().indefinitely()).flatMap(Collection::stream)
                .collect(Collectors.toSet());
        assertThat(sets).hasSize(4);
        sources.forEach(KafkaSource::closeQuietly);
    }

    @Test
    public void testWithMultipleConsumersWithAMultipleConsumeGroup() throws Exception {
        int count = 100;
        CountDownLatch latch = new CountDownLatch(count * partitions);

        List<KafkaSource<Integer, String>> sources = new ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + i)
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaSource<Integer, String> source = createSource(config, groupId);
            sources.add(source);
            source.getStream()
                    .invoke(rec -> {
                        onReceive(rec);
                        latch.countDown();
                    })
                    .subscribe().with(IncomingKafkaRecord::ack);
            await().until(() -> !source.getConsumer().getAssignments().await().indefinitely().isEmpty());
        }
        sendMessages(0, count);
        waitForMessages(latch);

        assertThat(sources).hasSize(4);
        assertThat(sources)
                .allSatisfy(s -> assertThat(s.getConsumer().getAssignments().await().indefinitely()).hasSize(partitions));

        sources.forEach(KafkaSource::closeQuietly);
    }

    @Test
    @Disabled("to be investigated - seems to fail on CI once in a while")
    public void testGroupingRecordsByPartition() throws Exception {
        int count = 10000;

        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        KafkaSource<Integer, String> source = createSource(config, groupId);
        Multi<IncomingKafkaRecord<Integer, String>> stream = source.getStream();

        CountDownLatch latch = new CountDownLatch(count);

        AtomicInteger concurrentPartitionExecutions = new AtomicInteger();
        Map<Integer, String> inProgressMap = new ConcurrentHashMap<>();

        int maxProcessingMs = 5;
        this.receiveTimeoutMillis = maxProcessingMs * count + 5000;

        stream
                .group().by(IncomingKafkaRecord::getPartition)
                .subscribe().with(recFromPartition -> recFromPartition
                        .emitOn(Infrastructure.getDefaultWorkerPool())
                        .subscribe().with(record -> {
                            int partition = record.getPartition();
                            String current = Thread.currentThread().getName() + ":" + record.getOffset();
                            String inProgress = inProgressMap.putIfAbsent(partition, current);
                            if (inProgress != null) {
                                concurrentPartitionExecutions.incrementAndGet();
                            }
                            onReceive(record);
                            latch.countDown();
                            record.ack();
                            inProgressMap.remove(partition);
                        }));

        waitFoPartitionAssignment();
        sendMessages(0, count);
        waitForMessages(latch);
        assertThat(concurrentPartitionExecutions.get()).isEqualTo(0);
        checkConsumedMessages(0, count);
    }

    @Test
    @Disabled
    public void testPausingWhileBrokerIsDown() throws Exception {
        try (StrimziKafkaContainer kafka = new StrimziKafkaContainer(KafkaBrokerExtension.KAFKA_VERSION)) {
            kafka.start();
            await().until(kafka::isRunning);
            Integer port = kafka.getMappedPort(KAFKA_PORT);
            sendMessages(0, 10, kafka.getBootstrapServers());
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                    .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
            createSource(config, groupId);
            Multi<IncomingKafkaRecord<Integer, String>> stream = source.getStream();

            AssertSubscriber<IncomingKafkaRecord<Integer, String>> subscriber = stream
                    .onItem().invoke(item -> CompletableFuture.runAsync(item::ack))
                    .subscribe().withSubscriber(AssertSubscriber.create(0));

            waitFoPartitionAssignment();

            await().untilAsserted(() -> subscriber.assertSubscribed().assertHasNotReceivedAnyItem());

            subscriber.request(1);
            await().until(() -> subscriber.getItems().size() == 1);
            await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());

            sendMessages(0, 10, kafka.getBootstrapServers());

            kafka.stop();
            await().until(() -> !kafka.isRunning());

            await().until(() -> !source.getConsumer().paused().await().indefinitely().isEmpty());
            subscriber.request(3);
            await().until(() -> subscriber.getItems().size() == 4);

            subscriber.request(10);
            AtomicInteger last = new AtomicInteger(subscriber.getItems().size());
            // Make sure we can't poll anymore.
            await()
                    .pollDelay(Duration.ofMillis(1000))
                    .until(() -> {
                        return last.get() == last.getAndSet(subscriber.getItems().size());
                    });

            try (StrimziKafkaContainer restarted = startKafkaBroker(port)) {
                await().until(restarted::isRunning);

                subscriber.request(100);
                await().until(() -> source.getConsumer().paused().await().indefinitely().isEmpty());

                sendMessages(10, 45, restarted.getBootstrapServers());
                await().until(() -> subscriber.getItems().size() == 55);
            }
        }
    }

    @Test
    @Disabled
    public void testResumingPausingWhileBrokerIsDown() throws Exception {
        try (StrimziKafkaContainer kafka = new StrimziKafkaContainer(KafkaBrokerExtension.KAFKA_VERSION)) {
            kafka.start();
            await().until(kafka::isRunning);
            Integer port = kafka.getMappedPort(KAFKA_PORT);
            String groupId = UUID.randomUUID().toString();
            MapBasedConfig config = createConsumerConfig(groupId)
                    .with("topic", topic)
                    .with(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                    .with(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
            createSource(config, groupId);
            source
                    .getStream().subscribe().withSubscriber(AssertSubscriber.create(1));

            waitFoPartitionAssignment();

            ReactiveKafkaConsumer<Integer, String> consumer = source.getConsumer();

            await().until(() -> !consumer.getAssignments().await().indefinitely().isEmpty());
            assertThat(consumer.pause().await().indefinitely()).isNotEmpty();

            consumer.resume().await().indefinitely();
            assertThat(consumer.paused().await().indefinitely()).isEmpty();

            kafka.stop();
            await().until(() -> !kafka.isRunning());

            assertThat(consumer.pause().await().indefinitely()).isNotEmpty();
            assertThat(consumer.resume().await().indefinitely());
            assertThat(consumer.paused().await().indefinitely()).isEmpty();
        }
    }

    private void onCommit(IncomingKafkaRecord<?, ?> record, CountDownLatch commitLatch, long[] committedOffsets) {
        committedOffsets[record.getPartition()] = record.getOffset() + 1;
        commitLatch.countDown();
    }

    private void onCommit(List<IncomingKafkaRecord<?, ?>> records, CountDownLatch commitLatch,
            long[] committedOffsets) {
        for (IncomingKafkaRecord<?, ?> rec : records) {
            committedOffsets[rec.getPartition()] = rec.getOffset() + 1;
            commitLatch.countDown();
        }
        records.clear();
    }

    private void sendAndWaitForMessages(Multi<IncomingKafkaRecord<Integer, String>> stream, int count)
            throws Exception {
        CountDownLatch receiveLatch = new CountDownLatch(count);
        subscribe(stream, receiveLatch);
        sendMessages(0, count);
        waitForMessages(receiveLatch);
    }

    private void checkCommitCallbacks(CountDownLatch commitLatch, long[] committedOffsets) throws InterruptedException {
        assertThat(commitLatch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS)).isTrue();
        for (int i = 0; i < partitions; i++) {
            assertThat(committedOffsets[i]).isEqualTo(receivedMessages.get(i).size());
        }
    }

    private void waitForCommits(KafkaSource<Integer, String> source, int count) {
        ReactiveKafkaConsumer<Integer, String> client = source.getConsumer();
        await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> assertThat(committedCount(client)).isEqualTo(count));
    }

    private void restartAndCheck(MapBasedConfig config, String groupId,
            int maxRedelivered) throws Exception {
        Thread.sleep(500);
        cancelSubscriptions();
        clearReceivedMessages();
        Multi<IncomingKafkaRecord<Integer, String>> stream = createSource(config, groupId).getStream();
        sendReceiveWithRedelivery(stream, maxRedelivered);
        clearReceivedMessages();
        cancelSubscriptions();
    }

    private void sendReceiveWithRedelivery(Multi<IncomingKafkaRecord<Integer, String>> stream,
            int maxRedelivered) throws Exception {

        CountDownLatch latch = new CountDownLatch(100 + maxRedelivered);
        subscribe(stream, latch);
        sendMessages(100, 100);

        for (int i = 0; i < partitions; i++) {
            int p = i;
            await().until(() -> receivedMessages.get(p).size() > 0);
        }

        for (int i = 100 - maxRedelivered; i < 100; i++) {
            int partition = i % partitions;
            if (receivedMessages.get(partition).get(0) > i) {
                latch.countDown();
            }
        }

        // Wait for messages, redelivered as well as those sent here
        waitForMessages(latch);

        // Within the range including redelivered, check that all messages were delivered.
        for (int i = 0; i < partitions; i++) {
            List<Integer> received = receivedMessages.get(i);
            int receiveStartIndex = received.get(0);
            int receiveEndIndex = received.get(received.size() - 1);
            checkConsumedMessages(i, receiveStartIndex, receiveEndIndex);
        }
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

    private void cancelSubscriptions() {
        subscriptions.forEach(Cancellable::cancel);
        subscriptions.clear();
    }

    private void sendReceive(Multi<IncomingKafkaRecord<Integer, String>> stream,
            int sendStartIndex, int sendCount,
            int receiveStartIndex, int receiveCount) throws Exception {

        CountDownLatch latch = new CountDownLatch(receiveCount);
        subscribe(stream, latch);
        if (sendCount > 0) {
            sendMessages(sendStartIndex, sendCount);
        }
        waitForMessages(latch);
        checkConsumedMessages(receiveStartIndex, receiveCount);
    }

    private void sendReceiveWithSendDelay(Multi<IncomingKafkaRecord<Integer, String>> stream,
            Duration sendDelay,
            int startIndex, int count) throws Exception {

        CountDownLatch latch = new CountDownLatch(count);
        subscribe(stream, latch);
        Thread.sleep(sendDelay.toMillis());
        sendMessages(startIndex, count);
        waitForMessages(latch);
        checkConsumedMessages(startIndex, count);
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

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        return source;
    }

    public KafkaSource<Integer, String> createSourceSeekToBeginning() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToBeginning());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        return source;
    }

    public KafkaSource<Integer, String> createSourceSeekToEnd() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToEnd());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        return source;
    }

    public KafkaSource<Integer, String> createSourceSeekToOffset() {
        String groupId = UUID.randomUUID().toString();
        MapBasedConfig config = createConsumerConfig(groupId)
                .put("topic", topic);

        SingletonInstance<KafkaConsumerRebalanceListener> listeners = new SingletonInstance<>(groupId,
                getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToOffset());

        source = new KafkaSource<>(vertx, groupId, new KafkaConnectorIncomingConfiguration(config),
                listeners, CountKafkaCdiEvents.noCdiEvents, UnsatisfiedInstance.instance(), 0);
        return source;
    }

    private KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignation() {
        return new KafkaConsumerRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                ReactiveKafkaConsumerTest.this.onPartitionsAssigned(partitions);
            }
        };
    }

    private KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToBeginning() {
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

    private KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToEnd() {
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

    private KafkaConsumerRebalanceListener getKafkaConsumerRebalanceListenerAwaitingAssignationAndSeekToOffset() {
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

    private void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assertThat(topic).isEqualTo(partitions.iterator().next().topic());
        assignSemaphore.release(partitions.size());
    }

    private void waitForMessages(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(receiveTimeoutMillis, TimeUnit.MILLISECONDS)) {
            fail(latch.getCount() + " messages not received, received=" + count(receivedMessages) + " : "
                    + receivedMessages);
        }
    }

    private void subscribe(Multi<IncomingKafkaRecord<Integer, String>> stream, CountDownLatch... latches)
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
        waitFoPartitionAssignment();
    }

    private void waitFoPartitionAssignment() throws InterruptedException {
        Assertions.assertTrue(
                assignSemaphore.tryAcquire(sessionTimeoutMillis + 1000, TimeUnit.MILLISECONDS), "Partitions not assigned");
    }

}
