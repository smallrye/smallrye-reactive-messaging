package io.smallrye.reactive.messaging.kafka;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.providers.helpers.PausableMulti;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ThrottledConcurrencyTest extends KafkaCompanionTestBase {

    @Test
    public void testUnorderedParallelProcessing() {
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, 5);

        // Produce messages to different partitions
        companion.produceStrings()
                .fromRecords(IntStream.range(0, 10).boxed().flatMap(i -> Stream.of(
                        new ProducerRecord<>(topic, 0, "A", "A-" + i),
                        new ProducerRecord<>(topic, 1, "B", "B-" + i),
                        new ProducerRecord<>(topic, 2, "C", "C-" + i),
                        new ProducerRecord<>(topic, 3, "D", "D-" + i),
                        new ProducerRecord<>(topic, 4, "E", "E-" + i)
                )).toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.unordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-unordered")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("commit-strategy", "throttled")
                .withPrefix("")
                .with("smallrye.messaging.worker.my-pool.max-concurrency", 5);

        UnorderedParallelConsumer app = runApplication(config, UnorderedParallelConsumer.class);
        long start = System.currentTimeMillis();
        System.out.println("Started processing messages " + start);

        // Wait for all messages to be processed
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> assertThat(app.received()).hasSize(50));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        assertThat(app.maxConcurrency()).isEqualTo(5);
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
        addBeans(KeyBasedOrderingDecorator.class);
        // Create topic and produce messages with different keys
        companion.topics().createAndWait(topic, 5);

        // Produce messages to different keys
        companion.produceStrings()
                .fromRecords(IntStream.range(0, 10).boxed().flatMap(i -> Stream.of(
                        new ProducerRecord<>(topic, 0, "A", "A-" + i),
                        new ProducerRecord<>(topic, 1, "B", "B-" + i),
                        new ProducerRecord<>(topic, 2, "C", "C-" + i),
                        new ProducerRecord<>(topic, 3, "D", "D-" + i),
                        new ProducerRecord<>(topic, 4, "E", "E-" + i)
                )).toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.key-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-key")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.processing-order", "ordered_by_key")
                .with("commit-strategy", "throttled");

        OrderedByKeyParallelConsumer app = runApplication(config, OrderedByKeyParallelConsumer.class);
        long start = System.currentTimeMillis();
        System.out.println("Started processing messages " + start);

        // Wait for all messages to be processed
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> assertThat(app.received()).hasSize(50));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        // With ordered-by-key, messages with the same key should be processed sequentially
        // So max concurrent processing per key should be 1
        assertThat(app.keyMaxCounter("A")).hasValue(1);
        assertThat(app.keyMaxCounter("B")).hasValue(1);
        assertThat(app.keyMaxCounter("C")).hasValue(1);
        assertThat(app.keyMaxCounter("D")).hasValue(1);
        assertThat(app.keyMaxCounter("E")).hasValue(1);
        assertThat(app.maxConcurrency()).isEqualTo(5);
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
            System.out.println(Thread.currentThread() + " Consumed message: " + payload + " from partition: " + metadata.getPartition() + ":"
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
        addBeans(PartitionBasedOrderingDecorator.class);
        // Create topic with 2 partitions
        companion.topics().createAndWait(topic, 5);

        // Produce messages to different partitions
        companion.produceStrings()
                .fromRecords(IntStream.range(0, 10).boxed().flatMap(i -> Stream.of(
                        new ProducerRecord<>(topic, 0, "key-" + i, "p0-" + i),
                        new ProducerRecord<>(topic, 1, "key-" + i, "p1-" + i),
                        new ProducerRecord<>(topic, 2, "key-" + i, "p2-" + i),
                        new ProducerRecord<>(topic, 3, "key-" + i, "p3-" + i),
                        new ProducerRecord<>(topic, 4, "key-" + i, "p4-" + i)
                )).toList())
                .awaitCompletion();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.partition-ordered")
                .with("topic", topic)
                .with("group.id", "test-throttled-ordered-by-partition")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("key.deserializer", StringDeserializer.class.getName())
                .with("throttled.processing-order", "ordered_by_partition")
                .with("commit-strategy", "throttled");

        OrderedByPartitionParallelConsumer app = runApplication(config, OrderedByPartitionParallelConsumer.class);
        long start = System.currentTimeMillis();
        System.out.println("Started processing messages " + start);

        // Wait for all messages to be processed
        await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> assertThat(app.received()).hasSize(50));
        long duration = System.currentTimeMillis() - start;
        System.out.println("Processing duration: " + duration + " ms");

        // With ordered-by-partition, messages from the same partition should be processed sequentially
        // So max concurrent processing per partition should be 1
        assertThat(app.partitionMaxCounter(0)).hasValue(1);
        assertThat(app.partitionMaxCounter(1)).hasValue(1);
        assertThat(app.partitionMaxCounter(2)).hasValue(1);
        assertThat(app.partitionMaxCounter(3)).hasValue(1);
        assertThat(app.partitionMaxCounter(4)).hasValue(1);
        assertThat(app.maxConcurrency()).isEqualTo(5);
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
            System.out.println(Thread.currentThread() + "Consumed message: " + payload + " from partition: " + metadata.getPartition() + ":"
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

    @ApplicationScoped
    private static class KeyBasedOrderingDecorator implements PublisherDecorator {

        @Override
        public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
                boolean isConnector) {
            if (isConnector) {
                return publisher
                        .group().by(message -> TopicPartitionKey.ofKey(getMetadata(message).getRecord()))
                        .onItem().transformToMulti(g -> {
                            PausableMulti<? extends Message<?>> pausable = new PausableMulti<>(g, false);
                            return pausable
                                    .invoke(pausable::pause)
                                    .map(m -> m.withAck(() -> m.ack().thenRun(pausable::resume)));
                        })
                        .merge(10);
            }
            return PublisherDecorator.super.decorate(publisher, channelName, isConnector);
        }

    }

    @ApplicationScoped
    private static class PartitionBasedOrderingDecorator implements PublisherDecorator {

        @Override
        public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
                boolean isConnector) {
            if (isConnector) {
                return publisher.group()
                        .by(message -> TopicPartitionKey.ofPartition(getMetadata(message).getRecord()))
                        .onItem().transformToMulti(g -> {
                            PausableMulti<? extends Message<?>> pausable = new PausableMulti<>(g, false);
                            return pausable
                                    .invoke(pausable::pause)
                                    .map(m -> m.withAck(() -> m.ack().thenRun(pausable::resume)));
                        })
                        .merge(128);
            }
            return PublisherDecorator.super.decorate(publisher, channelName, isConnector);
        }

    }

    private static IncomingKafkaRecordMetadata getMetadata(Message<?> message) {
        return message.getMetadata(IncomingKafkaRecordMetadata.class).get();
    }

}
