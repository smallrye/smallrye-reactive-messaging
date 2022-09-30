package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_CAUSE;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_OFFSET;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_PARTITION;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_REASON;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

public class BatchFailureHandlerTest extends KafkaCompanionTestBase {

    @Test
    public void testFailStrategy() {
        MyBatchReceiverBean bean = runApplication(getFailConfig(topic), MyBatchReceiverBean.class);

        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).contains(0, 1, 2, 3);

        await().until(() -> !isAlive());

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testFailStrategyWithPayload() {
        MyBatchReceiverBeanUsingPayload bean = runApplication(getFailConfig(topic),
                MyBatchReceiverBeanUsingPayload.class);

        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).contains(0, 1, 2, 3);

        await().until(() -> !isAlive());

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testIgnoreStrategy() {
        MyBatchReceiverBean bean = runApplication(getIgnoreConfig(topic), MyBatchReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testIgnoreStrategyWithPayload() {
        MyBatchReceiverBean bean = runApplication(getIgnoreConfig(topic), MyBatchReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testDeadLetterQueueStrategyWithBatchRecords() {
        String dqTopic = topic + "-dead-letter-topic";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dqTopic);

        MyBatchReceiverBean bean = runApplication(getDeadLetterQueueConfig(topic, dqTopic), MyBatchReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().until(() -> bean.list().size() == 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().until(() -> records.count() == bean.nacked().size());
        List<String> dlqOffsets = bean.nacked().stream().map(String::valueOf).collect(Collectors.toList());
        assertThat(records.getRecords()).extracting(ConsumerRecord::value).containsExactlyElementsOf(bean.nacked());
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dqTopic);
            assertThat(r.value()).isIn(bean.nacked());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack all -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn(dlqOffsets);
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomTopicAndMethodUsingPayload() {
        String dqTopic = topic + "-dead-letter-topic";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dqTopic);

        MyBatchReceiverBeanUsingPayload bean = runApplication(
                getDeadLetterQueueWithCustomConfig(topic, dqTopic),
                MyBatchReceiverBeanUsingPayload.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().until(() -> bean.list().size() == 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().until(() -> records.getRecords().size() == bean.nacked().size());
        List<String> dlqOffsets = bean.nacked().stream().map(String::valueOf).collect(Collectors.toList());
        assertThat(records.getRecords()).extracting(ConsumerRecord::value).containsExactlyElementsOf(bean.nacked());
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dqTopic);
            assertThat(r.value()).isIn(bean.nacked());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack all -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn(dlqOffsets);
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    private KafkaMapBasedConfig getFailConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", topic + "-group");
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "fail");
        config.put("batch", true);
        config.put("max.poll.records", 3);

        return config;
    }

    private KafkaMapBasedConfig getIgnoreConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", topic + "-group");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "ignore");
        config.put("batch", true);
        config.put("max.poll.records", 3);

        return config;
    }

    private KafkaMapBasedConfig getDeadLetterQueueConfig(String topic, String dq) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", "batch-dead-letter-default-group");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dq);
        config.put("batch", true);
        config.put("max.poll.records", 3);

        return config;
    }

    private KafkaMapBasedConfig getDeadLetterQueueWithCustomConfig(String topic, String dq) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", topic + "-group");
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dq);
        config.put("dead-letter-queue.key.serializer", IntegerSerializer.class.getName());
        config.put("dead-letter-queue.value.serializer", IntegerSerializer.class.getName());
        config.put("batch", true);
        config.put("max.poll.records", 3);

        return config;
    }

    @ApplicationScoped
    public static class MyBatchReceiverBean {
        private final List<Integer> received = new ArrayList<>();
        private final List<Integer> nacked = new ArrayList<>();

        private final LongAdder observedConsumerEvents = new LongAdder();
        private final LongAdder observedProducerEvents = new LongAdder();

        private volatile Function<List<Integer>, Throwable> toThrowable = payload -> new IllegalArgumentException(
                "nack all - " + payload);

        public void afterConsumerCreated(@Observes Consumer<?, ?> consumer) {
            observedConsumerEvents.increment();
        }

        public void afterProducerCreated(@Observes Producer<?, ?> producer) {
            observedProducerEvents.increment();
        }

        public void setToThrowable(Function<List<Integer>, Throwable> toThrowable) {
            this.toThrowable = toThrowable;
        }

        @Incoming("kafka")
        public CompletionStage<Void> process(KafkaRecordBatch<String, Integer> batchRecords) {
            List<Integer> payloads = batchRecords.getPayload();
            received.addAll(payloads);
            if (payloads.stream().anyMatch(v -> v != 0 && v % 3 == 0)) {
                nacked.addAll(payloads);
                return batchRecords.nack(toThrowable.apply(payloads));
            }
            return batchRecords.ack();
        }

        public List<Integer> list() {
            return received;
        }

        public List<Integer> nacked() {
            return nacked;
        }

        public long consumers() {
            return observedConsumerEvents.sum();
        }

        public long producers() {
            return observedProducerEvents.sum();
        }
    }

    @ApplicationScoped
    public static class MyBatchReceiverBeanUsingPayload {
        private final List<Integer> received = new ArrayList<>();
        private final List<Integer> nacked = new ArrayList<>();
        private final LongAdder observedConsumerEvents = new LongAdder();
        private final LongAdder observedProducerEvents = new LongAdder();

        public void afterConsumerCreated(@Observes Consumer<?, ?> consumer) {
            observedConsumerEvents.increment();
        }

        public void afterProducerCreated(@Observes Producer<?, ?> producer) {
            observedProducerEvents.increment();
        }

        @Incoming("kafka")
        public Uni<Void> process(List<Integer> values) {
            received.addAll(values);
            if (values.stream().anyMatch(v -> v != 0 && v % 3 == 0)) {
                nacked.addAll(values);
                return Uni.createFrom().failure(new IllegalArgumentException("nack all - " + values));
            }
            return Uni.createFrom().nullItem();
        }

        public List<Integer> list() {
            return received;
        }

        public List<Integer> nacked() {
            return nacked;
        }

        public long consumers() {
            return observedConsumerEvents.sum();
        }

        public long producers() {
            return observedProducerEvents.sum();
        }
    }

    public static class IdentityInterceptor<K, V> implements ConsumerInterceptor<K, V> {
        @Override
        public ConsumerRecords<K, V> onConsume(
                ConsumerRecords<K, V> records) {
            return records;
        }

        @Override
        public void onCommit(
                Map<TopicPartition, OffsetAndMetadata> offsets) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }
}
