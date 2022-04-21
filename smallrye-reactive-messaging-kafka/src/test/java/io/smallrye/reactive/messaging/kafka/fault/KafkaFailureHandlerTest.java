package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

public class KafkaFailureHandlerTest extends KafkaCompanionTestBase {

    @Test
    public void testFailStrategy() {
        MyReceiverBean bean = runApplication(getFailConfig(topic), MyReceiverBean.class);

        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        await().until(() -> !isAlive());

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testFailStrategyWithPayload() {
        MyReceiverBeanUsingPayload bean = runApplication(getFailConfig(topic),
                MyReceiverBeanUsingPayload.class);

        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        await().until(() -> !isAlive());

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testIgnoreStrategy() {
        MyReceiverBean bean = runApplication(getIgnoreConfig(topic), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testIgnoreStrategyWithPayload() {
        MyReceiverBean bean = runApplication(getIgnoreConfig(topic), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(0);
    }

    @Test
    public void testDeadLetterQueueStrategyWithDefaultTopic() {

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics("dead-letter-topic-kafka", 3);

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>("dead-letter-default", i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("dead-letter-topic-kafka");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo("dead-letter-default");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn("3", "6", "9");
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithMessageLessThrowable() {
        String dqTopic = topic + "-dead-letter-topic";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dqTopic, 3);

        MyReceiverBean bean = runApplication(
                getDeadLetterQueueWithCustomConfig(topic, dqTopic),
                MyReceiverBean.class);
        bean.setToThrowable(p -> new IllegalArgumentException());
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dqTopic);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value()))
                    .isEqualTo(new IllegalArgumentException().toString());
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn("3", "6", "9");
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomTopicAndMethodUsingPayload() {
        String dqTopic = topic + "-dead-letter-topic";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dqTopic, 3);

        MyReceiverBeanUsingPayload bean = runApplication(
                getDeadLetterQueueWithCustomConfig(topic, dqTopic),
                MyReceiverBeanUsingPayload.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dqTopic);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn("3", "6", "9");
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithInterceptor() {
        String dqTopic = topic + "-dead-letter-topic";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dqTopic, 3);

        MyReceiverBeanUsingPayload bean = runApplication(
                getDeadLetterQueueWithCustomConfig(topic, dqTopic)
                        .with("interceptor.classes", IdentityInterceptor.class.getName()),
                MyReceiverBeanUsingPayload.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dqTopic);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn("3", "6", "9");
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomConfig() {
        String dlTopic = topic + "-missed";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dlTopic, 3);

        MyReceiverBean bean = runApplication(getDeadLetterQueueWithCustomConfig(topic, dlTopic),
                MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlTopic);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn("3", "6", "9");
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    private KafkaMapBasedConfig getFailConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "fail");

        return config;
    }

    private KafkaMapBasedConfig getIgnoreConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "ignore");

        return config;
    }

    private KafkaMapBasedConfig getDeadLetterQueueConfig() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", "dead-letter-default");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");

        return config;
    }

    private KafkaMapBasedConfig getDeadLetterQueueWithCustomConfig(String topic, String dq) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dq);
        config.put("dead-letter-queue.key.serializer", IntegerSerializer.class.getName());
        config.put("dead-letter-queue.value.serializer", IntegerSerializer.class.getName());

        return config;
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private final List<Integer> received = new ArrayList<>();

        private final LongAdder observedConsumerEvents = new LongAdder();
        private final LongAdder observedProducerEvents = new LongAdder();

        private volatile Function<Integer, Throwable> toThrowable = payload -> new IllegalArgumentException(
                "nack 3 - " + payload);

        public void afterConsumerCreated(@Observes Consumer<?, ?> consumer) {
            observedConsumerEvents.increment();
        }

        public void afterProducerCreated(@Observes Producer<?, ?> producer) {
            observedProducerEvents.increment();
        }

        public void setToThrowable(Function<Integer, Throwable> toThrowable) {
            this.toThrowable = toThrowable;
        }

        @Incoming("kafka")
        public CompletionStage<Void> process(KafkaRecord<String, Integer> record) {
            Integer payload = record.getPayload();
            received.add(payload);
            if (payload != 0 && payload % 3 == 0) {
                return record.nack(toThrowable.apply(payload));
            }
            return record.ack();
        }

        public List<Integer> list() {
            return received;
        }

        public long consumers() {
            return observedConsumerEvents.sum();
        }

        public long producers() {
            return observedProducerEvents.sum();
        }
    }

    @ApplicationScoped
    public static class MyReceiverBeanUsingPayload {
        private final List<Integer> received = new ArrayList<>();
        private final LongAdder observedConsumerEvents = new LongAdder();
        private final LongAdder observedProducerEvents = new LongAdder();

        public void afterConsumerCreated(@Observes Consumer<?, ?> consumer) {
            observedConsumerEvents.increment();
        }

        public void afterProducerCreated(@Observes Producer<?, ?> producer) {
            observedProducerEvents.increment();
        }

        @Incoming("kafka")
        public Uni<Void> process(int value) {
            received.add(value);
            if (value != 0 && value % 3 == 0) {
                return Uni.createFrom().failure(new IllegalArgumentException("nack 3 - " + value));
            }
            return Uni.createFrom().nullItem();
        }

        public List<Integer> list() {
            return received;
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
