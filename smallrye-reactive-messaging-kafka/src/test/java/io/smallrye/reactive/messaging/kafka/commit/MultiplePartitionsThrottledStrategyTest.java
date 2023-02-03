package io.smallrye.reactive.messaging.kafka.commit;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.Vertx;
import reactor.core.publisher.Flux;

public class MultiplePartitionsThrottledStrategyTest extends KafkaCompanionTestBase {

    @Test
    public void testWithPartitions() {
        companion.topics().createAndWait(topic, 3);
        String sinkTopic = topic + "-sink";
        companion.topics().createAndWait(sinkTopic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "throttled")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("mp.messaging.outgoing.sink")
                .with("connector", "smallrye-kafka")
                .with("topic", sinkTopic)
                .with("value.serializer", IntegerSerializer.class.getName());

        ProcessorBean application = runApplication(config, ProcessorBean.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);

        companion.consumeIntegers().fromTopics(sinkTopic, expected)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    public void testWithPartitionsBlockingUnordered() {
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "throttled")
                .with("value.deserializer", IntegerDeserializer.class.getName());

        BlockingUnorderedBean application = runApplication(config, BlockingUnorderedBean.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);
    }

    @Test
    public void testWithPartitionsBlocking() {
        companion.topics().createAndWait(topic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "throttled")
                .with("value.deserializer", IntegerDeserializer.class.getName());

        BlockingBean application = runApplication(config, BlockingBean.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);
    }

    @Test
    public void testWithPartitionsStreamProcessor() {
        companion.topics().createAndWait(topic, 3);
        String sinkTopic = topic + "-sink";
        companion.topics().createAndWait(sinkTopic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "throttled")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("mp.messaging.outgoing.sink")
                .with("connector", "smallrye-kafka")
                .with("topic", sinkTopic)
                .with("value.serializer", IntegerSerializer.class.getName());

        StreamProcessingBean application = runApplication(config, StreamProcessingBean.class);

        int expected = 3000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);

        companion.consumeIntegers().fromTopics(sinkTopic, expected)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    @Disabled
    public void testWithPartitionsStreamProcessorFlux() {
        companion.topics().createAndWait(topic, 3);
        String sinkTopic = topic + "-sink";
        companion.topics().createAndWait(sinkTopic, 3);
        String groupId = UUID.randomUUID().toString();

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("partitions", 3)
                .with("auto.offset.reset", "earliest")
                .with("commit-strategy", "throttled")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("mp.messaging.outgoing.sink")
                .with("connector", "smallrye-kafka")
                .with("topic", sinkTopic)
                .with("value.serializer", IntegerSerializer.class.getName());

        PublisherStreamProcessingBean application = runApplication(config, PublisherStreamProcessingBean.class);

        int expected = 2000;
        Random random = new Random();
        companion.produceIntegers().usingGenerator(i -> {
            int p = random.nextInt(3);
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));

        await().atMost(1, TimeUnit.MINUTES)
                .until(() -> application.count() >= expected);

        companion.consumeIntegers().fromTopics(sinkTopic, expected)
                .awaitCompletion(Duration.ofMinutes(1));
    }

    @ApplicationScoped
    public static class ProcessorBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Outgoing("sink")
        public Message<Integer> consume(KafkaRecord<String, Integer> msg) {
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.withPayload(msg.getPayload() + 1)
                    .addMetadata(OutgoingKafkaRecordMetadata.builder().withPartition(msg.getPartition()).build());
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class StreamProcessingBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Outgoing("sink")
        public Multi<Message<Integer>> process(Multi<KafkaRecord<String, Integer>> multi) {
            return multi.map(msg -> {
                String k = Thread.currentThread().getName();
                List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
                list.add(msg.getPayload());
                count.incrementAndGet();
                assert msg.getContextMetadata().get().context() == Vertx.currentContext();
                return msg.withPayload(msg.getPayload() + 1)
                        .addMetadata(OutgoingKafkaRecordMetadata.builder().withPartition(msg.getPartition()).build());
            });
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class PublisherStreamProcessingBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Outgoing("sink")
        public Flux<Message<Integer>> process(Flux<KafkaRecord<String, Integer>> pub) throws InterruptedException {
            return pub.map(msg -> {
                String k = Thread.currentThread().getName();
                List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
                list.add(msg.getPayload());
                count.incrementAndGet();
                assert msg.getContextMetadata().get().context() == Vertx.currentContext();
                return msg.withPayload(msg.getPayload() + 1)
                        .addMetadata(OutgoingKafkaRecordMetadata.builder().withPartition(msg.getPartition()).build());
            });
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class BlockingBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Blocking
        public CompletionStage<Void> consume(Message<Integer> msg) throws InterruptedException {
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

    @ApplicationScoped
    public static class BlockingUnorderedBean {
        private final AtomicLong count = new AtomicLong();
        private final Map<String, List<Integer>> received = new ConcurrentHashMap<>();

        @Incoming("kafka")
        @Blocking(ordered = false)
        public CompletionStage<Void> consume(Message<Integer> msg) throws InterruptedException {
            String k = Thread.currentThread().getName();
            List<Integer> list = received.computeIfAbsent(k, s -> new CopyOnWriteArrayList<>());
            list.add(msg.getPayload());
            count.incrementAndGet();
            return msg.ack();
        }

        public Map<String, List<Integer>> getReceived() {
            return received;
        }

        public long count() {
            return count.get();
        }
    }

}
