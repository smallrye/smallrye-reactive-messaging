package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ThrottledConcurrencyTest extends KafkaCompanionTestBase {

    int partitions = 10;
    int concurrency = 10;
    int recordsParPartition = 100;

    @Test
    public void testUnorderedParallelProcessing() {
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, partitions);

        // Produce messages to different partitions
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed().flatMap(i -> IntStream.range(0, partitions).boxed()
                        .map(p -> new ProducerRecord<>(topic, p, "key-" + p, "value-" + i)))
                        .toList())
                .awaitCompletion();

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
        System.out.println("Started processing messages " + start);

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSize(partitions * recordsParPartition));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        assertThat(app.maxConcurrency()).isEqualTo(concurrency);
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
            System.out.println(Thread.currentThread() + " Current concurrency: " + conc);
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            Thread.sleep(500);
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
    public void testOrderedByKeyProcessing() {
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, partitions);

        // Produce messages to different keys
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed().flatMap(i -> IntStream.range(0, partitions).boxed()
                        .map(p -> new ProducerRecord<>(topic, p, "key-" + p, "value-" + i)))
                        .toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.key-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-key")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.processing-order", "ordered_by_key")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByKeyParallelConsumer app = runApplication(config, OrderedByKeyParallelConsumer.class);
        long start = System.currentTimeMillis();
        System.out.println("Started processing messages " + start);

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSize(partitions * recordsParPartition));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        // With ordered-by-key, messages with the same key should be processed sequentially
        // So max concurrent processing per key should be 1
        for (int i = 0; i < partitions; i++) {
            assertThat(app.keyMaxCounter("key-" + i)).hasValue(1);
        }
        assertThat(app.maxConcurrency()).isEqualTo(partitions);
    }

    @ApplicationScoped
    public static class OrderedByKeyParallelConsumer {

        Map<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> keyMaxCounter = new ConcurrentHashMap<>();

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("key-ordered")
        @Blocking(ordered = false)
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata) throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            AtomicInteger counter = keyCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            System.out.println(Thread.currentThread() + " Consumed message: " + payload + " from partition: "
                    + metadata.getPartition() + ":"
                    + metadata.getOffset() + " current concurrent: " + current);
            keyMaxCounter.computeIfAbsent(metadata.getKey(), x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));
            Thread.sleep(500);
            counter.decrementAndGet();
            concurrency.decrementAndGet();
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
    public void testOrderedByPartitionProcessing() {
        // Create topic with 2 partitions
        companion.topics().createAndWait(topic, partitions);

        // Produce messages to different partitions
        companion.produceStrings()
                .fromRecords(IntStream.range(0, recordsParPartition).boxed().flatMap(i -> IntStream.range(0, partitions).boxed()
                        .map(p -> new ProducerRecord<>(topic, p, "key-" + p, "value-" + i)))
                        .toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.partition-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-partition")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.processing-order", "ordered_by_partition")
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", concurrency);

        OrderedByPartitionParallelConsumer app = runApplication(config, OrderedByPartitionParallelConsumer.class);
        long start = System.currentTimeMillis();
        System.out.println("Started processing messages " + start);

        // Wait for all messages to be processed
        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(app.received())
                        .hasSize(partitions * recordsParPartition));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        // With ordered-by-partition, messages from the same partition should be processed sequentially
        // So max concurrent processing per partition should be 1
        for (int i = 0; i < partitions; i++) {
            assertThat(app.partitionMaxCounter(i)).hasValue(1);
        }
        assertThat(app.maxConcurrency()).isEqualTo(partitions);
    }

    @ApplicationScoped
    public static class OrderedByPartitionParallelConsumer {

        Map<Integer, AtomicInteger> partitionCounter = new ConcurrentHashMap<>();
        Map<Integer, AtomicInteger> partitionMaxCounter = new ConcurrentHashMap<>();
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger concurrency = new AtomicInteger(0);
        private final AtomicInteger maxConcurrency = new AtomicInteger(0);

        @Incoming("partition-ordered")
        @Blocking(ordered = false)
        public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata) throws InterruptedException {
            int conc = concurrency.incrementAndGet();
            maxConcurrency.updateAndGet(max -> Math.max(max, conc));
            received.add(payload);
            AtomicInteger counter = partitionCounter.computeIfAbsent(metadata.getPartition(), x -> new AtomicInteger(0));
            int current = counter.incrementAndGet();
            partitionMaxCounter.computeIfAbsent(metadata.getPartition(), x -> new AtomicInteger(0))
                    .updateAndGet(max -> Math.max(max, current));
            System.out.println(Thread.currentThread() + "Consumed message: " + payload + " from partition: "
                    + metadata.getPartition() + ":"
                    + metadata.getOffset() + " current concurrent: " + current);
            Thread.sleep(500);
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

}
