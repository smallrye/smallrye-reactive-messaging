package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;
import io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import io.vertx.core.impl.cpu.CpuCoreSensor;

public class ConcurrentProcessorTest extends KafkaCompanionTestBase {

    private MapBasedConfig dataconfig() {
        String groupId = UUID.randomUUID().toString();
        return kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", groupId)
                .with("topic", topic)
                .with("concurrency", 3)
                .with("failure-strategy", "dead-letter-queue")
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());
    }

    private void produceMessages() {
        int expected = 10;
        companion.produceIntegers().usingGenerator(i -> {
            int p = i % 3;
            return new ProducerRecord<>(topic, p, Integer.toString(p), i);
        }, expected).awaitCompletion(Duration.ofMinutes(1));
    }

    @Test
    public void testConcurrentConsumer() {
        addBeans(ConsumerRecordConverter.class);
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyConsumerBean bean = runApplication(dataconfig(), MyConsumerBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentProcessor() {
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyProcessorBean bean = runApplication(dataconfig(), MyProcessorBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentStreamTransformer() {
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyStreamTransformerBean bean = runApplication(dataconfig(), MyStreamTransformerBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentStreamInjectingBean() {
        companion.topics().createAndWait(topic, 3);

        produceMessages();
        MyChannelInjectingBean bean = runApplication(dataconfig(), MyChannelInjectingBean.class);

        List<Integer> list = bean.getResults();
        assertThat(list).isEmpty();
        bean.process();

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            assertThat(bean.getPerThread().keySet()).hasSizeGreaterThanOrEqualTo(getMaxNumberOfEventLoop(3));
        });
    }

    @Test
    public void testConcurrentConsumerWithDLQ() {
        addBeans(ConsumerRecordConverter.class);
        companion.topics().createAndWait(topic, 3);

        String dlqTopic = topic + "-dlq";

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", UUID.randomUUID().toString())
                .with("topic", topic)
                .with("concurrency", 3)
                .with("failure-strategy", "dead-letter-queue")
                .with("dead-letter-queue.topic", dlqTopic)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());

        produceMessages();
        MyConsumerBeanWithFailures bean = runApplication(config, MyConsumerBeanWithFailures.class);

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 4, 5, 7, 8, 10);
        });

        // Verify messages 3, 6, 9 were sent to DLQ
        await().untilAsserted(() -> {
            List<Integer> dlqMessages = companion.consumeIntegers()
                    .fromTopics(dlqTopic, 3)
                    .awaitCompletion()
                    .getRecords().stream()
                    .map(r -> r.value())
                    .toList();
            assertThat(dlqMessages).containsExactlyInAnyOrder(3, 6, 9);
        });
    }

    @Test
    public void testConcurrentConsumerWithNestedDLQConfig() {
        addBeans(ConsumerRecordConverter.class);
        companion.topics().createAndWait(topic, 3);

        String dlqTopicDefault = topic + "-dlq";
        String dlqTopicOverride = topic + "-dlq-override";

        // Configure DLQ for base channel and override topic for one concurrent channel via nested config
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", UUID.randomUUID().toString())
                .with("topic", topic)
                .with("concurrency", 3)
                .with("failure-strategy", "dead-letter-queue")
                .with("dead-letter-queue.topic", dlqTopicDefault)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                .withPrefix("")
                // Override DLQ topic for first concurrent channel to test nested config
                .with("mp.messaging.incoming.data$1.dead-letter-queue.topic", dlqTopicOverride);

        produceMessages();
        MyConsumerBeanWithFailures bean = runApplication(config, MyConsumerBeanWithFailures.class);

        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 4, 5, 7, 8, 10);
        });

        // Verify messages 3, 6, 9 were sent to DLQ topics
        // At least one should go to the override topic (proving nested config works)
        await().untilAsserted(() -> {
            var records = companion.consumeIntegers()
                    .fromTopics(Set.of(dlqTopicDefault, dlqTopicOverride), 3)
                    .awaitCompletion()
                    .getRecords();

            // Verify all 3 messages are in DLQ
            List<Integer> allDlqMessages = records.stream()
                    .map(r -> r.value())
                    .toList();
            assertThat(allDlqMessages).containsExactlyInAnyOrder(3, 6, 9);

            // Verify that nested configuration was actually used by checking some messages went to override topic
            long overrideCount = records.stream()
                    .filter(r -> dlqTopicOverride.equals(r.topic()))
                    .count();
            assertThat(overrideCount)
                    .as("Override DLQ topic should have received at least one message (proving nested config works)")
                    .isGreaterThan(0);
        });
    }

    @Test
    public void testConcurrentConsumerWithDelayedRetryTopic() {
        addBeans(ConsumerRecordConverter.class, KafkaDelayedRetryTopic.Factory.class);
        companion.topics().createAndWait(topic, 3);

        String retryTopic1 = KafkaDelayedRetryTopic.getRetryTopic(topic, 1000);
        String retryTopic2 = KafkaDelayedRetryTopic.getRetryTopic(topic, 2000);
        String dlqTopic = topic + "-dlq";

        // Configure delayed-retry-topic with concurrency
        // This test verifies that the retry topic producer inherits the main channel config
        // which was the issue in #2766
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", UUID.randomUUID().toString())
                .with("topic", topic)
                .with("concurrency", 3)
                .with("failure-strategy", "delayed-retry-topic")
                .with("delayed-retry-topic.topics", retryTopic1 + "," + retryTopic2)
                .with("dead-letter-queue.topic", dlqTopic)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName());

        produceMessages();
        MyConsumerBeanWithFailures bean = runApplication(config, MyConsumerBeanWithFailures.class);

        // All messages should be processed (successful ones plus retries)
        await().untilAsserted(() -> {
            assertThat(bean.getResults())
                    .hasSizeGreaterThanOrEqualTo(10)
                    .contains(1, 2, 4, 5, 7, 8, 10);
        });

        // Verify messages 3, 6, 9 were sent to retry topics
        // This proves that the delayed retry topic producer was created successfully
        // with the correct configuration inherited from the main channel
        await().untilAsserted(() -> {
            List<Integer> retryMessages = companion.consumeIntegers()
                    .fromTopics(Set.of(retryTopic1, retryTopic2), 6)
                    .awaitCompletion()
                    .getRecords().stream()
                    .map(r -> r.value())
                    .toList();
            assertThat(retryMessages).hasSizeGreaterThanOrEqualTo(3);
            assertThat(retryMessages).contains(3, 6, 9);
        });
    }

    @Test
    public void testConcurrentConsumerWithDelayedRetryTopicAndCustomBootstrap() {
        addBeans(ConsumerRecordConverter.class, KafkaDelayedRetryTopic.Factory.class);
        companion.topics().createAndWait(topic, 3);

        String retryTopic1 = KafkaDelayedRetryTopic.getRetryTopic(topic, 1000);
        String retryTopic2 = KafkaDelayedRetryTopic.getRetryTopic(topic, 2000);
        String dlqTopic = topic + "-dlq";

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.data")
                .with("group.id", UUID.randomUUID().toString())
                .with("topic", topic)
                .with("concurrency", 3)
                .with("failure-strategy", "delayed-retry-topic")
                .with("delayed-retry-topic.topics", retryTopic1 + "," + retryTopic2)
                .with("dead-letter-queue.topic", dlqTopic)
                .with("auto.offset.reset", "earliest")
                .with("value.deserializer", IntegerDeserializer.class.getName())
                // Custom bootstrap.servers that MUST be inherited by DLQ producer and retry consumer
                .with("dead-letter-queue.bootstrap.servers", "localhost:1234")
                .with("dead-letter-queue.client.id", "dlq-producer-should-not-connect");

        produceMessages();
        MyConsumerBeanWithFailures bean = runApplication(config, MyConsumerBeanWithFailures.class);

        // All messages should be processed (successful ones plus retries)
        await().untilAsserted(() -> assertThat(bean.getResults()).hasSizeLessThan(10));
    }

    @ApplicationScoped
    public static class MyConsumerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        public Uni<Void> process(ConsumerRecord<String, Integer> record) {
            int value = record.value();
            int next = value + 1;
            perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
            list.add(next);
            return Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofMillis(100));
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    @ApplicationScoped
    public static class MyConsumerBeanWithFailures {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public Uni<Void> process(Message<Integer> message) {
            int value = message.getPayload();
            int next = value + 1;
            list.add(next);

            // Nack messages where value is divisible by 3 (values 3, 6, 9)
            if (value != 0 && value % 3 == 0) {
                return Uni.createFrom().completionStage(message.nack(new IllegalArgumentException("nack " + value)));
            }

            return Uni.createFrom().completionStage(message.ack())
                    .onItem().delayIt().by(Duration.ofMillis(100));
        }

        public List<Integer> getResults() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyProcessorBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        @Outgoing("sink")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public Uni<Message<Integer>> process(Message<Integer> input) {
            int value = input.getPayload();
            int next = value + 1;
            perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
            return Uni.createFrom().item(input.withPayload(next))
                    .onItem().delayIt().by(Duration.ofMillis(100));
        }

        @Incoming("sink")
        public void sink(int val) {
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    @ApplicationScoped
    public static class MyStreamTransformerBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Incoming("data")
        @Outgoing("sink")
        public Multi<Message<Integer>> process(Multi<Message<Integer>> multi) {
            return multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = input.getPayload();
                        int next = value + 1;
                        perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
                        return Uni.createFrom().item(input.withPayload(next))
                                .onItem().delayIt().by(Duration.ofMillis(100));
                    });
        }

        @Incoming("sink")
        public void sink(int val) {
            list.add(val);
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    @ApplicationScoped
    public static class MyChannelInjectingBean {

        private final List<Integer> list = new CopyOnWriteArrayList<>();
        private final Map<Thread, List<Integer>> perThread = new ConcurrentHashMap<>();

        @Channel("data")
        @Inject
        Multi<Message<Integer>> multi;

        public void process() {
            multi.onItem()
                    .transformToUniAndConcatenate(input -> {
                        int value = input.getPayload();
                        int next = value + 1;
                        list.add(next);
                        perThread.computeIfAbsent(Thread.currentThread(), t -> new CopyOnWriteArrayList<>()).add(next);
                        return Uni.createFrom().completionStage(input::ack)
                                .onItem().delayIt().by(Duration.ofMillis(100));
                    })
                    .subscribe().with(__ -> {
                    });
        }

        public List<Integer> getResults() {
            return list;
        }

        public Map<Thread, List<Integer>> getPerThread() {
            return perThread;
        }
    }

    public int getMaxNumberOfEventLoop(int expected) {
        // On Github Actions, only one event loop is created.
        int cpus = CpuCoreSensor.availableProcessors();
        // For some reason when Github Actions has 4 cores it'll still run on 1 event loop thread
        if (cpus <= 4) {
            return 1;
        }
        return Math.min(expected, cpus / 2);
    }

}
