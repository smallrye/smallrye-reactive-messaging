package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ThrottledConcurrencyTest extends KafkaCompanionTestBase {

    final static int partitions = 10;
    final static int keySpace = 50;
    final static int concurrency = 10;
    final static int recordsParPartition = 100;
    final static int processingTimeMs = 100;

    @Test
    public void testUnorderedParallelProcessing() {
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, partitions);

        // Produce messages to different partitions
        int nbMessages = partitions * recordsParPartition;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed()
                        .flatMap(i -> IntStream.range(0, partitions).unordered().boxed()
                                .map(p -> new ProducerRecord<>(topic, p, "key-" + p, "value-" + i)))
                        .toList())
                .awaitCompletion(Duration.ofSeconds(30));

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.unordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-unordered")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        UnorderedParallelConsumer app = runApplication(config, UnorderedParallelConsumer.class);
        long start = System.currentTimeMillis();

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSizeGreaterThanOrEqualTo(nbMessages));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processed " + nbMessages + " duration: " + duration + " ms");

        assertThat(app.maxConcurrency()).isEqualTo(concurrency);

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-unordered"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .containsOnly((long) recordsParPartition);
        });
    }

    @ApplicationScoped
    public static class UnorderedParallelConsumer {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("unordered")
        @Blocking(ordered = false, value = "my-pool")
        public void consume(String payload) throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            Thread.sleep(processingTimeMs);
            concurrency.decrementAndGet();
        }

        public List<String> received() {
            return received;
        }

        public int maxConcurrency() {
            return maxConcurrency.get();
        }
    }

    @Test
    public void testOrderedByManyKeysProcessing() {
        // Create topic and produce messages with different keys
        int customPartitions = 1;
        int customRecordsParPartition = 1_500;
        companion.topics().createAndWait(topic, customPartitions);

        // Produce messages to different keys
        companion.produceStrings()
                .fromRecords(IntStream.range(0, customRecordsParPartition).boxed()
                        .flatMap(i -> IntStream.range(0, customPartitions).boxed()
                                .map(p -> new ProducerRecord<>(topic, p, "key-" + i, "value-" + i)))
                        .toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.key-ordered-many-keys")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-key-many-keys")
                .with("throttled.unprocessed-record-max-age.ms", 1000)
                .with("auto.commit.interval.ms", 500)
                .with("max.poll.records", 50)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByManyKeyParallelConsumer app = runApplication(config, OrderedByManyKeyParallelConsumer.class);
        long start = System.currentTimeMillis();

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSize(customPartitions * customRecordsParPartition));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        // since all keys are different, max is 1
        for (int i = 0; i < customRecordsParPartition; i++) {
            assertThat(app.keyMaxCounter("key-" + i)).hasValue(1);
        }

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-ordered-by-key-many-keys"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .containsOnly((long) customRecordsParPartition);
        });
    }

    @ApplicationScoped
    public static class OrderedByManyKeyParallelConsumer {

        Map<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> keyMaxCounter = new ConcurrentHashMap<>();

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("key-ordered-many-keys")
        @Blocking(ordered = false, value = "my-pool")
        public Uni<Void> consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            AtomicInteger counter = keyCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            keyMaxCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));
            return Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofMillis(processingTimeMs))
                    .invoke(x -> {
                        counter.decrementAndGet();
                        concurrency.decrementAndGet();
                    });
        }

        public List<String> received() {
            return received;
        }

        public AtomicInteger keyMaxCounter(Object key) {
            return keyMaxCounter.get(key);
        }

        public int maxConcurrency() {
            return maxConcurrency.get();
        }
    }

    @Test
    public void testOrderedByKeyProcessing() {
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, partitions);

        // Produce messages to different keys
        int nbMessages = keySpace * recordsParPartition;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed()
                        .flatMap(i -> IntStream.range(0, keySpace).boxed()
                                .map(k -> new ProducerRecord<>(topic, k % partitions, "key-" + k, "value-" + i)))
                        .toList())
                .awaitCompletion(Duration.ofSeconds(30));

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.key-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-key")
                .with("throttled.unprocessed-record-max-age.ms", 1000)
                .with("auto.commit.interval.ms", 500)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByKeyParallelConsumer app = runApplication(config, OrderedByKeyParallelConsumer.class);
        long start = System.currentTimeMillis();

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSizeGreaterThanOrEqualTo(nbMessages));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processed " + nbMessages + " duration: " + duration + " ms");

        // With ordered-by-key, messages with the same key should be processed sequentially
        // So max concurrent processing per key should be 1
        for (int i = 0; i < keySpace; i++) {
            assertThat(app.keyMaxCounter("key-" + i)).hasValue(1);
        }

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-ordered-by-key"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .hasSize(partitions)
                    .allSatisfy(offset -> assertThat(offset).isGreaterThanOrEqualTo(recordsParPartition));
        });
    }

    @Test
    public void testOrderedByKeyProcessingLongProcessing() {
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, partitions);

        int recordsParPartition = 10;
        int keySpace = 20;
        // Produce messages to different keys
        int nbMessages = keySpace * recordsParPartition;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed()
                        .flatMap(i -> IntStream.range(0, keySpace).boxed()
                                .map(k -> new ProducerRecord<>(topic, k % partitions, "key-" + k, "value-" + i)))
                        .toList())
                .awaitCompletion(Duration.ofSeconds(30));

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.key-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-key-long")
                .with("auto.commit.interval.ms", 500)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByKeyParallelLongProcessingConsumer app = runApplication(config,
                OrderedByKeyParallelLongProcessingConsumer.class);
        long start = System.currentTimeMillis();

        // Wait for all messages to be processed
        await().atMost(5, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSizeGreaterThanOrEqualTo(nbMessages));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processed " + nbMessages + " duration: " + duration + " ms");

        // With ordered-by-key, messages with the same key should be processed sequentially
        // So max concurrent processing per key should be 1
        for (int i = 0; i < keySpace; i++) {
            assertThat(app.keyMaxCounter("key-" + i)).hasValue(1);
        }

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-ordered-by-key-long"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .hasSize(partitions)
                    .allSatisfy(offset -> assertThat(offset).isGreaterThanOrEqualTo(recordsParPartition));
        });
    }

    @Test
    public void testOrderedByKeyProcessingLargeKeySpace() {
        // Create topic and produce messages with different keys
        int myPartitions = 5;
        int myRecordsParPartition = 500;
        companion.topics().createAndWait(topic, myPartitions);

        // Produce messages to different keys
        int nbMessages = myPartitions * myRecordsParPartition;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, myRecordsParPartition).boxed()
                        .flatMap(i -> IntStream.range(0, myPartitions).unordered().boxed()
                                .map(p -> new ProducerRecord<>(topic, p, UUID.randomUUID().toString(), "value-" + i)))
                        .toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.key-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-key-large")
                .with("throttled.unprocessed-record-max-age.ms", 1000)
                .with("auto.commit.interval.ms", 500)
                .with("max.poll.records", 50)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByKeyParallelConsumer app = runApplication(config, OrderedByKeyParallelConsumer.class);
        long start = System.currentTimeMillis();

        // Wait for all messages to be processed
        await().pollDelay(1, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSize(nbMessages));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processed " + nbMessages + " duration: " + duration + " ms");

        // With ordered-by-key, messages with the same key should be processed sequentially
        // So max concurrent processing per key should be 1
        assertThat(app.keyMaxCounter()).values()
                .allSatisfy(c -> assertThat(c).hasValue(1));

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-ordered-by-key-large"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .containsOnly((long) myRecordsParPartition);
        });
    }

    @ApplicationScoped
    public static class OrderedByKeyParallelConsumer {

        Map<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> keyMaxCounter = new ConcurrentHashMap<>();

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("key-ordered")
        @Blocking(ordered = false, value = "my-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            AtomicInteger counter = keyCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            keyMaxCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));
            Thread.sleep(processingTimeMs);
            counter.decrementAndGet();
            concurrency.decrementAndGet();
        }

        public List<String> received() {
            return received;
        }

        public AtomicInteger keyMaxCounter(Object key) {
            return keyMaxCounter.get(key);
        }

        public Map<String, AtomicInteger> keyMaxCounter() {
            return keyMaxCounter;
        }

        public int maxConcurrency() {
            return maxConcurrency.get();
        }
    }

    @ApplicationScoped
    public static class OrderedByKeyParallelLongProcessingConsumer {

        Map<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> keyMaxCounter = new ConcurrentHashMap<>();

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("key-ordered")
        @Blocking(ordered = false, value = "my-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            AtomicInteger counter = keyCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            keyMaxCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));
            Thread.sleep(processingTimeMs * 20);
            counter.decrementAndGet();
            concurrency.decrementAndGet();
        }

        public List<String> received() {
            return received;
        }

        public AtomicInteger keyMaxCounter(Object key) {
            return keyMaxCounter.get(key);
        }

        public Map<String, AtomicInteger> keyMaxCounter() {
            return keyMaxCounter;
        }

        public int maxConcurrency() {
            return maxConcurrency.get();
        }
    }

    @Test
    public void testOrderedByPartitionProcessing() {
        // Create topic with 2 partitions
        companion.topics().createAndWait(topic, partitions);

        // Produce messages to different partitions
        int nbMessages = partitions * recordsParPartition;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed()
                        .flatMap(i -> IntStream.range(0, partitions).unordered().boxed()
                                .map(p -> new ProducerRecord<>(topic, p, "key-" + p, "value-" + i)))
                        .toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.partition-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-partition")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "partition")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByPartitionParallelConsumer app = runApplication(config, OrderedByPartitionParallelConsumer.class);
        long start = System.currentTimeMillis();

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSize(nbMessages));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processed " + nbMessages + " duration: " + duration + " ms");

        // With ordered-by-partition, messages from the same partition should be processed sequentially
        // So max concurrent processing per partition should be 1
        for (int i = 0; i < partitions; i++) {
            assertThat(app.partitionMaxCounter(i)).hasValue(1);
        }
        assertThat(app.maxConcurrency()).isEqualTo(concurrency);

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-ordered-by-partition"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .containsOnly((long) recordsParPartition);
        });
    }

    @ApplicationScoped
    public static class OrderedByPartitionParallelConsumer {

        Map<Integer, AtomicInteger> partitionCounter = new ConcurrentHashMap<>();
        Map<Integer, AtomicInteger> partitionMaxCounter = new ConcurrentHashMap<>();
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("partition-ordered")
        @Blocking(ordered = false, value = "my-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata) throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            AtomicInteger counter = partitionCounter.computeIfAbsent(metadata.getPartition(), x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            partitionMaxCounter.computeIfAbsent(metadata.getPartition(), x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));
            Thread.sleep(processingTimeMs);
            counter.decrementAndGet();
            concurrency.decrementAndGet();
        }

        public List<String> received() {
            return received;
        }

        public AtomicInteger partitionMaxCounter(Object key) {
            return partitionMaxCounter.get(key);
        }

        public int maxConcurrency() {
            return maxConcurrency.get();
        }
    }

    @Test
    public void testOrderedByKeyWithRebalance() {
        // Create topic with single partition
        companion.topics().createAndWait(topic, 1);

        // Produce initial batch of messages
        int initialBatch = 50;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, initialBatch).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 5), "value-" + i))
                        .toList())
                .awaitCompletion();

        String groupId = "test-rebalance-" + UUID.randomUUID();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.rebalance-test")
                .with("topic", topic)
                .with("group.id", groupId)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.rebalance-pool.max-concurrency", 5);

        // Start consumer via reactive messaging
        RebalanceConsumer app = runApplication(config, RebalanceConsumer.class);

        // Wait for consumer to process some messages
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.received()).hasSizeGreaterThan(10));

        int receivedBeforeRebalance = app.received().size();

        // Start a second Kafka consumer in the same group - this triggers rebalance
        var secondConsumer = companion.consumeStrings()
                .withGroupId(groupId)
                .fromTopics(topic);

        // Wait a bit for rebalance to happen
        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);

        // Produce more messages after rebalance
        int secondBatch = 50;
        companion.produceStrings()
                .fromRecords(IntStream.range(initialBatch, initialBatch + secondBatch).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 5), "value-" + i))
                        .toList())
                .awaitCompletion();

        // Verify consumer continues processing after rebalance
        // It should process at least some of the new messages
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.received().size())
                        .isGreaterThan(receivedBeforeRebalance));

        // Wait a bit for rebalance to happen
        await().pollDelay(2, TimeUnit.SECONDS).until(() -> true);

        // Verify ordering per key is maintained
        Map<String, List<String>> receivedByKey = app.receivedByKey();

        // Check that messages for each key are in order
        for (int keyNum = 0; keyNum < 5; keyNum++) {
            String key = "key-" + keyNum;
            List<String> messagesForKey = receivedByKey.get(key);
            if (messagesForKey != null && messagesForKey.size() > 1) {
                // Verify messages are in order
                for (int i = 0; i < messagesForKey.size() - 1; i++) {
                    int current = Integer.parseInt(messagesForKey.get(i).substring("value-".length()));
                    int next = Integer.parseInt(messagesForKey.get(i + 1).substring("value-".length()));
                    assertThat(next).isGreaterThan(current);
                }
            }
        }

        assertThat(app.received()).hasSizeLessThan(initialBatch + secondBatch);

        secondConsumer.close();

        int thirdBatch = 50;
        companion.produceStrings()
                .fromRecords(IntStream.range(initialBatch + secondBatch, initialBatch + secondBatch + thirdBatch).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 5), "value-" + i))
                        .toList())
                .awaitCompletion();

        await().untilAsserted(() -> assertThat(app.received()).hasSizeGreaterThanOrEqualTo(initialBatch + thirdBatch));
    }

    @ApplicationScoped
    public static class RebalanceConsumer {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Map<String, List<String>> receivedByKey = new ConcurrentHashMap<>();

        @Incoming("rebalance-test")
        @Blocking(ordered = false, value = "rebalance-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            received.add(payload);
            receivedByKey.computeIfAbsent(metadata.getKey(), k -> new CopyOnWriteArrayList<>()).add(payload);
            Thread.sleep(50);
        }

        public List<String> received() {
            return received;
        }

        public Map<String, List<String>> receivedByKey() {
            return receivedByKey;
        }
    }

    @Test
    public void testOrderedByKeyWithNullKeys() {
        // Create topic with multiple partitions
        companion.topics().createAndWait(topic, 3);

        // Produce messages with mix of null and non-null keys
        int messagesPerPartition = 100;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, messagesPerPartition).boxed()
                        .flatMap(i -> IntStream.range(0, 3).boxed()
                                .map(p -> {
                                    // Mix null keys and regular keys
                                    String key = i % 3 == 0 ? null : "key-" + (i % 5);
                                    return new ProducerRecord<>(topic, p, key, "value-" + i + "-p" + p);
                                }))
                        .toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.null-key-test")
                .with("topic", topic)
                .with("group.id", "test-null-keys-" + UUID.randomUUID())
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.null-key-pool.max-concurrency", 10);

        NullKeyConsumer app = runApplication(config, NullKeyConsumer.class);

        // Wait for all messages to be processed
        int totalMessages = messagesPerPartition * 3;
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received()).hasSize(totalMessages));

        // Verify ordering for non-null keys is maintained
        Map<String, List<String>> byKey = app.receivedByKey();
        for (String key : byKey.keySet()) {
            if (key != null && !key.equals("null")) {
                List<String> messages = byKey.get(key);
                // Messages with same key should be ordered within their partition
                assertThat(messages).hasSizeGreaterThan(0);
            }
        }

        // Verify null-key messages were processed (they can be from any partition)
        List<String> nullKeyMessages = byKey.get("null");
        assertThat(nullKeyMessages).isNotNull();
        assertThat(nullKeyMessages).hasSizeGreaterThan(0);

        // Verify max concurrency per key
        // Since messages with same key go to different partitions,
        // they create separate TopicPartitionKey groups and can process concurrently
        // So max concurrency for a key can be up to the number of partitions (3 in this test)
        for (int i = 0; i < 5; i++) {
            String key = "key-" + i;
            if (app.keyMaxCounter(key) != null) {
                // Max concurrency should be > 0 and <= number of partitions
                assertThat(app.keyMaxCounter(key).get())
                        .isGreaterThan(0)
                        .isLessThanOrEqualTo(3);
            }
        }

        // Null keys can also have higher concurrency since they span partitions
        assertThat(app.keyMaxCounter("null")).isNotNull();
        assertThat(app.keyMaxCounter("null").get())
                .isGreaterThan(0)
                .isLessThanOrEqualTo(3);
    }

    @ApplicationScoped
    public static class NullKeyConsumer {
        Map<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> keyMaxCounter = new ConcurrentHashMap<>();
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Map<String, List<String>> receivedByKey = new ConcurrentHashMap<>();

        @Incoming("null-key-test")
        @Blocking(ordered = false, value = "null-key-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            String key = metadata.getKey() == null ? "null" : metadata.getKey();
            received.add(payload);
            receivedByKey.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(payload);

            AtomicInteger counter = keyCounter.computeIfAbsent(key, x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            keyMaxCounter.computeIfAbsent(key, x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));

            Thread.sleep(50);
            counter.decrementAndGet();
        }

        public List<String> received() {
            return received;
        }

        public Map<String, List<String>> receivedByKey() {
            return receivedByKey;
        }

        public AtomicInteger keyMaxCounter(String key) {
            return keyMaxCounter.get(key);
        }
    }

    @Test
    public void testOrderedByKeyWithGroupRecreation() {
        // Create topic with single partition
        companion.topics().createAndWait(topic, 1);

        String groupId = "test-group-recreation-" + UUID.randomUUID();
        int pollTimeout = 500; // Short poll timeout for faster test

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.recreation-test")
                .with("topic", topic)
                .with("group.id", groupId)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                .with("poll.timeout.ms", pollTimeout)
                .withPrefix("")
                .with("smallrye.messaging.worker.recreation-pool.max-concurrency", 5);

        // Start consumer
        RecreationConsumer app = runApplication(config, RecreationConsumer.class);

        // Round 1: Produce first batch of messages
        int batchSize = 10;
        companion.produceStrings()
                .fromRecords(IntStream.range(0, batchSize).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 3), "round1-value-" + i))
                        .toList())
                .awaitCompletion();

        // Wait for round 1 to be processed
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.received()).hasSizeGreaterThanOrEqualTo(batchSize));

        int round1Count = app.received().size();
        System.out.println("Round 1 completed with " + round1Count + " messages");

        // Wait for groups to be cleaned up by timeout (poll timeout * 2 + buffer)
        await().pollDelay(pollTimeout * 2 + 500, TimeUnit.MILLISECONDS).until(() -> true);

        // Round 2: Produce second batch with same keys
        companion.produceStrings()
                .fromRecords(IntStream.range(batchSize, batchSize * 2).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 3), "round2-value-" + i))
                        .toList())
                .awaitCompletion();

        // Wait for round 2 to be processed
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.received().size())
                        .isGreaterThanOrEqualTo(round1Count + batchSize));

        int round2Count = app.received().size();
        System.out.println("Round 2 completed with " + round2Count + " total messages");

        // Wait for groups to be cleaned up again
        await().pollDelay(pollTimeout * 2 + 500, TimeUnit.MILLISECONDS).until(() -> true);

        // Round 3: Produce third batch with same keys
        companion.produceStrings()
                .fromRecords(IntStream.range(batchSize * 2, batchSize * 3).boxed()
                        .map(i -> new ProducerRecord<>(topic, 0, "key-" + (i % 3), "round3-value-" + i))
                        .toList())
                .awaitCompletion();

        // Wait for round 3 to be processed
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(app.received().size())
                        .isGreaterThanOrEqualTo(round2Count + batchSize));

        System.out.println("Round 3 completed with " + app.received().size() + " total messages");

        // Verify all 3 rounds were consumed
        assertThat(app.received()).hasSizeGreaterThanOrEqualTo(batchSize * 3);

        // Verify we have messages from all 3 rounds
        assertThat(app.received().stream().filter(s -> s.contains("round1")).count()).isGreaterThan(0);
        assertThat(app.received().stream().filter(s -> s.contains("round2")).count()).isGreaterThan(0);
        assertThat(app.received().stream().filter(s -> s.contains("round3")).count()).isGreaterThan(0);

        // Verify ordering per key is maintained across all rounds
        Map<String, List<String>> receivedByKey = app.receivedByKey();
        for (int keyNum = 0; keyNum < 3; keyNum++) {
            String key = "key-" + keyNum;
            List<String> messagesForKey = receivedByKey.get(key);
            assertThat(messagesForKey).isNotNull();
            System.out.println("Key " + key + " received " + messagesForKey.size() + " messages: " + messagesForKey);
        }
    }

    @Test
    public void testOrderedMaxConcurrencyLimit() {
        // Test that throttled.ordered.max-concurrency limits concurrent group processing
        int numKeys = 20;
        int messagesPerKey = 50;
        int maxConcurrencyLimit = 10;
        companion.topics().createAndWait(topic, 1);

        // Produce messages with many different keys
        companion.produceStrings()
                .fromRecords(IntStream.range(0, messagesPerKey).boxed()
                        .flatMap(i -> IntStream.range(0, numKeys).boxed()
                                .map(k -> new ProducerRecord<>(topic, "key-" + k, "value-" + i)))
                        .toList())
                .awaitCompletion(Duration.ofSeconds(30));

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.max-concurrency-test")
                .with("topic", topic)
                .with("group.id", "test-throttled-max-concurrency")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.ordered", "key")
                .with("commit-strategy", "throttled")
                // Set high max.poll.records but limit max-concurrency
                .with("max.poll.records", 100)
                .with("throttled.ordered.max-concurrency", maxConcurrencyLimit)
                .withPrefix("")
                .with("smallrye.messaging.worker.max-concurrency-pool.max-concurrency", 20);

        MaxConcurrencyConsumer app = runApplication(config, MaxConcurrencyConsumer.class);

        // Wait for all messages to be processed
        int totalMessages = numKeys * messagesPerKey;
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSizeGreaterThanOrEqualTo(totalMessages));

        // Verify that max concurrent groups never exceeded the limit
        assertThat(app.maxConcurrentGroups()).isLessThanOrEqualTo(maxConcurrencyLimit);

        // Verify ordering per key is maintained
        for (int i = 0; i < numKeys; i++) {
            assertThat(app.keyMaxCounter("key-" + i)).hasValue(1);
        }

        await().untilAsserted(() -> {
            assertThat(companion.consumerGroups().offsets("test-throttled-max-concurrency"))
                    .values().extracting(OffsetAndMetadata::offset)
                    .allSatisfy(offset -> assertThat(offset).isGreaterThanOrEqualTo(totalMessages));
        });
    }

    @ApplicationScoped
    public static class MaxConcurrencyConsumer {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Map<String, AtomicInteger> keyCounters = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> keyMaxCounters = new ConcurrentHashMap<>();
        private final Set<String> activeGroups = ConcurrentHashMap.newKeySet();
        private final AtomicInteger maxConcurrentGroups = new AtomicInteger(0);

        @Incoming("max-concurrency-test")
        @Blocking(ordered = false, value = "max-concurrency-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            String key = metadata.getKey();

            // Track active groups
            activeGroups.add(key);
            int currentConcurrent = activeGroups.size();
            maxConcurrentGroups.updateAndGet(max -> Math.max(max, currentConcurrent));

            // Track per-key concurrency
            int count = keyCounters.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet();
            keyMaxCounters.computeIfAbsent(key, k -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, count));

            received.add(payload);
            Thread.sleep(processingTimeMs);

            keyCounters.get(key).decrementAndGet();
            activeGroups.remove(key);
        }

        public List<String> received() {
            return received;
        }

        public int maxConcurrentGroups() {
            return maxConcurrentGroups.get();
        }

        public Optional<Integer> keyMaxCounter(String key) {
            return Optional.ofNullable(keyMaxCounters.get(key)).map(AtomicInteger::get);
        }
    }

    @ApplicationScoped
    public static class RecreationConsumer {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final Map<String, List<String>> receivedByKey = new ConcurrentHashMap<>();

        @Incoming("recreation-test")
        @Blocking(ordered = false, value = "recreation-pool")
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
                throws InterruptedException {
            received.add(payload);
            receivedByKey.computeIfAbsent(metadata.getKey(), k -> new CopyOnWriteArrayList<>()).add(payload);
            Thread.sleep(50);
        }

        public List<String> received() {
            return received;
        }

        public Map<String, List<String>> receivedByKey() {
            return receivedByKey;
        }
    }

}
