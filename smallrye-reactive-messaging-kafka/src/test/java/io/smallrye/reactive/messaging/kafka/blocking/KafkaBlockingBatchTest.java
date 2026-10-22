package io.smallrye.reactive.messaging.kafka.blocking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class KafkaBlockingBatchTest extends KafkaCompanionTestBase {

    private KafkaMapBasedConfig config() {
        return config(false);
    }

    private KafkaMapBasedConfig flatBatchConfig() {
        return config(true);
    }

    private KafkaMapBasedConfig config(boolean flatBatch) {
        String sinkTopic = topic + "-out";
        KafkaMapBasedConfig c = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic);
        if (flatBatch) {
            c.put("flat-batch", true);
        }
        return c.withPrefix("mp.messaging.outgoing.sink")
                .put("connector", "smallrye-kafka")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", sinkTopic);
    }

    private KafkaMapBasedConfig subscriberConfig() {
        return subscriberConfig(false);
    }

    private KafkaMapBasedConfig flatBatchSubscriberConfig() {
        return subscriberConfig(true);
    }

    private KafkaMapBasedConfig subscriberConfig(boolean flatBatch) {
        KafkaMapBasedConfig c = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic);
        if (flatBatch) {
            c.put("flat-batch", true);
        }
        return c;
    }

    @Test
    public void testBlockingBatchProcessorWithKafka() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        BatchProcessor bean = runApplication(config(), BatchProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
        assertThat(bean.getResults()).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, messageCount)
                        .map(i -> i + 1)
                        .boxed()
                        .toList());

        List<String> threads = bean.getThreads().stream().distinct().toList();
        for (String name : threads) {
            assertThat(name).startsWith("vert.x-worker-thread-");
        }
    }

    @Test
    public void testBlockingBatchProcessorWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        BatchProcessor bean = runApplication(flatBatchConfig(), BatchProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
        assertThat(bean.getResults()).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, messageCount)
                        .map(i -> i + 1)
                        .boxed()
                        .toList());
    }

    @Test
    public void testBlockingBatchProcessorWithFlatBatchHighVolume() {
        int messageCount = 5000;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(30));

        BatchProcessor bean = runApplication(flatBatchConfig(), BatchProcessor.class);

        await().atMost(Duration.ofMinutes(1))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    @Test
    public void testBlockingBatchProcessorHighVolume() {
        int messageCount = 5000;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(30));

        BatchProcessor bean = runApplication(config(), BatchProcessor.class);

        await().atMost(Duration.ofMinutes(1))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);

        List<String> threads = bean.getThreads().stream().distinct().toList();
        for (String name : threads) {
            assertThat(name).startsWith("vert.x-worker-thread-");
        }
    }

    @Test
    public void testBlockingBatchProcessorSlowProcessing() {
        int messageCount = 200;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        SlowBatchProcessor bean = runApplication(config(), SlowBatchProcessor.class);

        await().atMost(Duration.ofMinutes(2))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
        assertThat(bean.getResults()).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, messageCount)
                        .map(i -> i + 1)
                        .boxed()
                        .toList());

        List<String> threads = bean.getThreads().stream().distinct().toList();
        for (String name : threads) {
            assertThat(name).startsWith("vert.x-worker-thread-");
        }
    }

    @Test
    public void testBlockingBatchProcessorFailureWithIgnoreStrategy() {
        int messageCount = 100;
        // Messages 0..99, processor throws on value 50
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        KafkaMapBasedConfig ignoreConfig = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic)
                .put("failure-strategy", "ignore")
                .withPrefix("mp.messaging.outgoing.sink")
                .put("connector", "smallrye-kafka")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic + "-out");

        FailingBatchProcessor bean = runApplication(ignoreConfig, FailingBatchProcessor.class);

        // All 100 messages should be processed (99 succeed + 1 ignored failure)
        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        // The failing message (50) should not appear in results
        assertThat(bean.getResults()).hasSize(messageCount - 1);
        assertThat(bean.getResults()).doesNotContain(51); // 50+1 would be 51
        assertThat(bean.getFailures()).hasSize(1);
        assertThat(bean.getFailures().get(0)).isEqualTo(50);
    }

    @Test
    public void testBlockingBatchProcessorFailureWithFailStop() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        // Default strategy is fail-stop
        FailingBatchProcessor bean = runApplication(config(), FailingBatchProcessor.class);

        // Should stop processing after failure at message 50
        // Messages before 50 should be processed, messages after 50 should not
        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.getFailures().size() >= 1);
        // Give a short window to confirm processing stopped
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Only messages before the failure should be processed
        assertThat(bean.count()).isLessThan(messageCount);
        assertThat(bean.getResults()).allSatisfy(v -> assertThat(v).isLessThanOrEqualTo(50));
    }

    // --- Gap #1: Subscriber with flat-batch ---

    @Test
    public void testBlockingBatchSubscriberWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        BatchSubscriber bean = runApplication(flatBatchSubscriberConfig(), BatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    // --- Gap #2: Failure tests with flat-batch ---

    @Test
    public void testBlockingBatchProcessorFailureWithIgnoreStrategyFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        KafkaMapBasedConfig ignoreConfig = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic)
                .put("failure-strategy", "ignore")
                .put("flat-batch", true)
                .withPrefix("mp.messaging.outgoing.sink")
                .put("connector", "smallrye-kafka")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic + "-out");

        FailingBatchProcessor bean = runApplication(ignoreConfig, FailingBatchProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount - 1);
        assertThat(bean.getResults()).doesNotContain(51);
        assertThat(bean.getFailures()).hasSize(1);
    }

    @Test
    public void testBlockingBatchProcessorFailureWithFailStopFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        FailingBatchProcessor bean = runApplication(flatBatchConfig(), FailingBatchProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.getFailures().size() >= 1);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(bean.count()).isLessThan(messageCount);
        assertThat(bean.getResults()).allSatisfy(v -> assertThat(v).isLessThanOrEqualTo(50));
    }

    // --- Send failure tests ---

    @Test
    public void testBlockingBatchProcessorSendFailure() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        KafkaMapBasedConfig badSinkConfig = kafkaConfig("mp.messaging.incoming.data")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .with("auto.offset.reset", "earliest")
                .put("topic", topic)
                .withPrefix("mp.messaging.outgoing.sink")
                .with("connector", "smallrye-kafka")
                .with("value.serializer", IntegerSerializer.class.getName())
                .with("topic", topic + "-out")
                .with("bootstrap.servers", "localhost:19199")
                .with("max-inflight-messages", 50)
                .with("delivery.timeout.ms", 1000)
                .with("request.timeout.ms", 500)
                .with("max.block.ms", 500)
                .with("retries", 0);

        BatchProcessor bean = runApplication(badSinkConfig, BatchProcessor.class);

        // Send failures trigger fail-stop nack which should make the consumer unhealthy
        await().atMost(Duration.ofSeconds(30))
                .until(() -> !isAlive());
        // Messages were processed before the failure propagated
        assertThat(bean.count()).isGreaterThan(0);
        // bean.count() messages were processed before failure
    }

    @Test
    public void testBlockingBatchProcessorSendFailureWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        KafkaMapBasedConfig badSinkConfig = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic)
                .put("flat-batch", true)
                .withPrefix("mp.messaging.outgoing.sink")
                .put("connector", "smallrye-kafka")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic + "-out")
                .put("bootstrap.servers", "localhost:19199")
                .put("max-inflight-messages", 50)
                .put("delivery.timeout.ms", 1000)
                .put("request.timeout.ms", 500)
                .put("max.block.ms", 500)
                .put("retries", 0);

        BatchProcessor bean = runApplication(badSinkConfig, BatchProcessor.class);

        // Send failures trigger fail-stop nack which should make the consumer unhealthy
        await().atMost(Duration.ofSeconds(30))
                .until(() -> !isAlive());
        assertThat(bean.count()).isGreaterThan(0);
        // bean.count() messages were processed before failure
    }

    @Test
    public void testBlockingSubscriberProcessingFailure() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        FailingBatchSubscriber bean = runApplication(subscriberConfig(), FailingBatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> !isAlive());
        // subscriber processed some messages before failure
        assertThat(bean.getFailures()).hasSize(1);
    }

    @Test
    public void testBlockingSubscriberProcessingFailureWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        FailingBatchSubscriber bean = runApplication(flatBatchSubscriberConfig(), FailingBatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> !isAlive());
        // subscriber processed some messages before failure
        assertThat(bean.getFailures()).hasSize(1);
    }

    @Test
    public void testNonBlockingProcessorSendFailure() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        KafkaMapBasedConfig badSinkConfig = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("max.poll.records", 10)
                .put("topic", topic)
                .withPrefix("mp.messaging.outgoing.sink")
                .put("connector", "smallrye-kafka")
                .put("value.serializer", IntegerSerializer.class.getName())
                .put("topic", topic + "-out")
                .put("bootstrap.servers", "localhost:19199")
                .put("max-inflight-messages", 50)
                .put("delivery.timeout.ms", 1000)
                .put("request.timeout.ms", 500)
                .put("max.block.ms", 500)
                .put("retries", 0);

        NonBlockingProcessor bean = runApplication(badSinkConfig, NonBlockingProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> !isAlive());
        assertThat(bean.count()).isGreaterThan(0);
        // non-blocking processed some messages before failure
    }

    // --- Gap #3: Message-returning processor with Kafka ---

    @Test
    public void testBlockingBatchMessageProcessor() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        MessageBatchProcessor bean = runApplication(config(), MessageBatchProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
        assertThat(bean.getResults()).containsExactlyElementsOf(
                java.util.stream.IntStream.range(0, messageCount)
                        .map(i -> i + 1)
                        .boxed()
                        .toList());
    }

    @Test
    public void testBlockingBatchMessageProcessorWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        MessageBatchProcessor bean = runApplication(flatBatchConfig(), MessageBatchProcessor.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    // --- Gap #4: CS/Uni-returning subscriber with Kafka ---

    @Test
    public void testBlockingBatchCompletionStageSubscriber() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        CompletionStageBatchSubscriber bean = runApplication(subscriberConfig(), CompletionStageBatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    @Test
    public void testBlockingBatchCompletionStageSubscriberWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        CompletionStageBatchSubscriber bean = runApplication(flatBatchSubscriberConfig(),
                CompletionStageBatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    @Test
    public void testBlockingBatchUniSubscriber() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        UniBatchSubscriber bean = runApplication(subscriberConfig(), UniBatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    @Test
    public void testBlockingBatchUniSubscriberWithFlatBatch() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        UniBatchSubscriber bean = runApplication(flatBatchSubscriberConfig(), UniBatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    // --- Existing tests ---

    @Test
    public void testBlockingBatchSubscriber() {
        int messageCount = 100;
        companion.produceIntegers()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), messageCount)
                .awaitCompletion(Duration.ofSeconds(10));

        KafkaMapBasedConfig subscriberConfig = kafkaConfig("mp.messaging.incoming.data")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic);

        BatchSubscriber bean = runApplication(subscriberConfig, BatchSubscriber.class);

        await().atMost(Duration.ofSeconds(30))
                .until(() -> bean.count() >= messageCount);
        assertThat(bean.getResults()).hasSize(messageCount);
    }

    @ApplicationScoped
    public static class NonBlockingProcessor {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Outgoing("sink")
        public int process(int value) {
            int result = value + 1;
            results.add(result);
            counter.incrementAndGet();
            return result;
        }

        public List<Integer> getResults() {
            return results;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class BatchProcessor {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<String> threads = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Outgoing("sink")
        @Blocking
        public int process(int value) {
            threads.add(Thread.currentThread().getName());
            int result = value + 1;
            results.add(result);
            counter.incrementAndGet();
            return result;
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<String> getThreads() {
            return threads;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class SlowBatchProcessor {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<String> threads = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Outgoing("sink")
        @Blocking
        public int process(int value) throws InterruptedException {
            Thread.sleep(50);
            threads.add(Thread.currentThread().getName());
            int result = value + 1;
            results.add(result);
            counter.incrementAndGet();
            return result;
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<String> getThreads() {
            return threads;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class FailingBatchProcessor {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<Integer> failures = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Outgoing("sink")
        @Blocking
        public int process(int value) {
            if (value == 50) {
                failures.add(value);
                counter.incrementAndGet();
                throw new RuntimeException("Simulated failure on value 50");
            }
            int result = value + 1;
            results.add(result);
            counter.incrementAndGet();
            return result;
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<Integer> getFailures() {
            return failures;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class MessageBatchProcessor {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Outgoing("sink")
        @Blocking
        public Message<Integer> process(Message<Integer> message) {
            int result = message.getPayload() + 1;
            results.add(result);
            counter.incrementAndGet();
            return message.withPayload(result);
        }

        public List<Integer> getResults() {
            return results;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class CompletionStageBatchSubscriber {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Blocking
        public CompletionStage<Void> consume(Message<Integer> message) {
            results.add(message.getPayload());
            counter.incrementAndGet();
            return message.ack();
        }

        public List<Integer> getResults() {
            return results;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class UniBatchSubscriber {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Blocking
        public io.smallrye.mutiny.Uni<Void> consume(int value) {
            results.add(value);
            counter.incrementAndGet();
            return io.smallrye.mutiny.Uni.createFrom().voidItem();
        }

        public List<Integer> getResults() {
            return results;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class FailingBatchSubscriber {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final List<Integer> failures = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Blocking
        public void consume(int value) {
            if (value == 50) {
                failures.add(value);
                counter.incrementAndGet();
                throw new RuntimeException("Simulated failure on value 50");
            }
            results.add(value);
            counter.incrementAndGet();
        }

        public List<Integer> getResults() {
            return results;
        }

        public List<Integer> getFailures() {
            return failures;
        }

        public long count() {
            return counter.get();
        }
    }

    @ApplicationScoped
    public static class BatchSubscriber {
        private final List<Integer> results = new CopyOnWriteArrayList<>();
        private final AtomicLong counter = new AtomicLong();

        @Incoming("data")
        @Blocking
        public void consume(int value) {
            results.add(value);
            counter.incrementAndGet();
        }

        public List<Integer> getResults() {
            return results;
        }

        public long count() {
            return counter.get();
        }
    }
}
