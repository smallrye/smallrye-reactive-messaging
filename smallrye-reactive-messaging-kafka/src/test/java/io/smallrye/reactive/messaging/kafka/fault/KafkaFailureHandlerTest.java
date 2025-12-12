package io.smallrye.reactive.messaging.kafka.fault;

import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DATA;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DESERIALIZER;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_DLQ;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_KEY_DATA;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_REASON;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_TOPIC;
import static io.smallrye.reactive.messaging.kafka.DeserializationFailureHandler.DESERIALIZATION_FAILURE_VALUE_DATA;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_CAUSE;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_CAUSE_CLASS_NAME;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_EXCEPTION_CLASS_NAME;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_OFFSET;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_PARTITION;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_REASON;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDeadLetterQueue.DEAD_LETTER_TOPIC;
import static io.smallrye.reactive.messaging.kafka.fault.KafkaDelayedRetryTopic.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.observation.DefaultMessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservation;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;
import io.smallrye.reactive.messaging.observation.ObservationContext;

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
        addBeans(MyObservationCollector.class);

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics("dead-letter-topic-kafka", 3);

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(topic), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("dead-letter-topic-kafka");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull().isIn("3", "6", "9");
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);

        MyObservationCollector collector = get(MyObservationCollector.class);
        await().untilAsserted(() -> assertThat(collector.observed()).hasSize(3)
                .allSatisfy(MessageObservation::isDone));
    }

    @ApplicationScoped
    public static class MyObservationCollector implements MessageObservationCollector<ObservationContext> {

        List<MessageObservation> observed = new CopyOnWriteArrayList<>();

        @Override
        public ObservationContext initObservation(String channel, boolean incoming, boolean emitter) {
            if (incoming) {
                return null;
            }
            return ObservationContext.DEFAULT;
        }

        @Override
        public MessageObservation onNewMessage(String channel, Message<?> message, ObservationContext observationContext) {
            DefaultMessageObservation observation = new DefaultMessageObservation(channel);
            observed.add(observation);
            return observation;
        }

        public List<MessageObservation> observed() {
            return observed;
        }
    }

    @Test
    public void testDeadLetterQueueStrategyWithDeserializationError() {
        String dlqTopic = topic + "-dlq";

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(dlqTopic, 10);

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(topic)
                .with("dead-letter-queue.topic", dlqTopic)
                .with("fail-on-deserialization-failure", false), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "boom-" + i), 10);

        String expectedReason = "Size of data received by IntegerDeserializer is not 4";
        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 10);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlqTopic);
            assertThat(r.value()).startsWith("boom-");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(RecordDeserializationException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).isEqualTo(expectedReason);
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull();
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_REASON).value())).isEqualTo(expectedReason);
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_DATA).value())).startsWith("boom-");
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_DESERIALIZER).value()))
                    .isEqualTo(IntegerDeserializer.class.getName());
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_TOPIC).value())).isEqualTo(topic);
        });

        assertThat(bean.list()).isEmpty();

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithDeserializationErrorAndFailureHandler() {
        String dlqTopic = topic + "-dlq";

        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(dlqTopic, 10);

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(topic)
                .with("dead-letter-queue.topic", dlqTopic)
                .with("fail-on-deserialization-failure", false), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "boom-" + i), 10);

        String expectedReason = "Size of data received by IntegerDeserializer is not 4";
        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 10);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlqTopic);
            assertThat(r.value()).startsWith("boom-");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(RecordDeserializationException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).isEqualTo(expectedReason);
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull();
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_REASON).value())).isEqualTo(expectedReason);
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_DATA).value())).startsWith("boom-");
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_DESERIALIZER).value()))
                    .isEqualTo(IntegerDeserializer.class.getName());
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_TOPIC).value())).isEqualTo(topic);
            assertThat(r.headers().lastHeader(DESERIALIZATION_FAILURE_DLQ)).isNull();
        });

        assertThat(bean.list()).isEmpty();

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(1);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueStrategyWithKeyDeserializationError() {
        String dlqTopic = topic + "-dlq";

        ConsumerTask<Integer, String> records = companion.consume(Integer.class, String.class)
                .fromTopics(dlqTopic, 10);

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(topic)
                .with("dead-letter-queue.topic", dlqTopic)
                .with("fail-on-deserialization-failure", false)
                .with("key.deserializer", DoubleDeserializer.class.getName()) // wrong key deserializer
                , MyReceiverBean.class);
        await().until(this::isReady);

        companion.produce(Integer.class, String.class)
                .usingGenerator(i -> new ProducerRecord<>(topic, i, "boom-" + i), 10);

        String expectedReason = "Size of data received by IntegerDeserializer is not 4";
        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 10);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlqTopic);
            assertThat(r.key()).isIn(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(r.value()).startsWith("boom-");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(RecordDeserializationException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).isEqualTo(expectedReason);
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_OFFSET).value())).isNotNull();
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_REASON).value())).isEqualTo(expectedReason);
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_KEY_DATA).value())).isNotNull();
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_VALUE_DATA).value())).startsWith("boom-");
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_DESERIALIZER).value()))
                    .isEqualTo(IntegerDeserializer.class.getName());
            assertThat(new String(r.headers().lastHeader(DESERIALIZATION_FAILURE_TOPIC).value())).isEqualTo(topic);
            assertThat(r.headers().lastHeader(DESERIALIZATION_FAILURE_DLQ)).isNull();
        });

        assertThat(bean.list()).isEmpty();

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
        bean.setToThrowable(p -> new IllegalStateException(new NullPointerException("msg")));
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dqTopic);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalStateException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value()))
                    .isEqualTo(new NullPointerException() + ": msg");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_CAUSE).value()))
                    .isEqualTo("msg");
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME).value()))
                    .isEqualTo(NullPointerException.class.getName());
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
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
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
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
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

    @Test
    public void testDeadLetterQueueConfigInheritance() {
        String dlTopic = topic + "-dlq";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dlTopic, 3);

        // Test config inheritance: set producer configs on main channel and verify DLQ inherits them
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dlTopic);

        // Set a producer config on the main channel - this should be inherited by DLQ
        config.put("compression.type", "gzip");
        config.put("acks", "all");
        config.put("linger.ms", "100");

        // Set a producer interceptor to capture the DLQ producer config
        config.put("dead-letter-queue.interceptor.classes", DLQConfigCapturingInterceptor.class.getName());

        MyReceiverBean bean = runApplication(config, MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlTopic);
            assertThat(r.value()).isIn(3, 6, 9);
        });

        // Verify that DLQ producer inherited the configs from main channel
        await().untilAsserted(() -> {
            Map<String, ?> dlqConfig = DLQConfigCapturingInterceptor.capturedConfig;
            assertThat(dlqConfig).isNotNull();
            assertThat(dlqConfig.get("compression.type")).isEqualTo("gzip");
            assertThat(dlqConfig.get("acks")).isEqualTo("all");
            assertThat(dlqConfig.get("linger.ms")).isEqualTo("100");
        });

        assertThat(isAlive()).isTrue();
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueConfigInheritanceWithOverride() {
        String dlTopic = topic + "-dlq";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dlTopic, 3);

        // Test config inheritance with override: DLQ-specific config should take precedence
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dlTopic);

        // Set producer configs on main channel
        config.put("compression.type", "gzip");
        config.put("acks", "1");
        config.put("linger.ms", "100");

        // Override one config specifically for DLQ - this should take precedence over inherited value
        config.put("dead-letter-queue.compression.type", "snappy");
        config.put("dead-letter-queue.acks", "all");
        // linger.ms should still be inherited as "100"

        // Set a producer interceptor to capture the DLQ producer config
        config.put("dead-letter-queue.interceptor.classes", DLQConfigCapturingInterceptor.class.getName());

        // Reset the captured config
        DLQConfigCapturingInterceptor.capturedConfig = null;

        MyReceiverBean bean = runApplication(config, MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlTopic);
            assertThat(r.value()).isIn(3, 6, 9);
        });

        // Verify that DLQ-specific config overrides inherited config, but other configs are still inherited
        await().untilAsserted(() -> {
            Map<String, ?> dlqConfig = DLQConfigCapturingInterceptor.capturedConfig;
            assertThat(dlqConfig).isNotNull();
            // DLQ-specific override should win
            assertThat(dlqConfig.get("compression.type")).isEqualTo("snappy");
            assertThat(dlqConfig.get("acks")).isEqualTo("all");
            // Inherited value should be used
            assertThat(dlqConfig.get("linger.ms")).isEqualTo("100");
        });

        assertThat(isAlive()).isTrue();
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDeadLetterQueueConfigInheritanceVsExternalConfig() {
        String dlTopic = topic + "-dlq";

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(dlTopic, 3);

        // Test that DLQ inherits from main channel config, NOT from external @Identifier config
        // This tests the scenario where external configs could interfere with inheritance
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "dead-letter-queue");
        config.put("dead-letter-queue.topic", dlTopic);

        // Set producer configs on main channel that should be inherited
        config.put("max.block.ms", "5000");
        config.put("request.timeout.ms", "10000");
        config.put("retries", "5");

        // Set a producer interceptor to capture the DLQ producer config
        config.put("dead-letter-queue.interceptor.classes", DLQConfigCapturingInterceptor.class.getName());

        // Reset the captured config
        DLQConfigCapturingInterceptor.capturedConfig = null;

        // Add a bean that provides external configs with DIFFERENT values
        // The DLQ should inherit from main channel, NOT from this external config
        addBeans(ExternalKafkaConfigProvider.class);

        MyReceiverBean bean = runApplication(config, MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.getRecords().size() == 3);
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo(dlTopic);
            assertThat(r.value()).isIn(3, 6, 9);
        });

        // Verify that DLQ inherited from main channel config, NOT from external config
        await().untilAsserted(() -> {
            Map<String, ?> dlqConfig = DLQConfigCapturingInterceptor.capturedConfig;
            assertThat(dlqConfig).isNotNull();
            // These should be inherited from main channel (5000, 10000, 5)
            // NOT from external config (99999, 88888, 99)
            assertThat(dlqConfig.get("max.block.ms"))
                    .as("max.block.ms should be inherited from main channel (5000), not external config (99999)")
                    .isEqualTo("5000");
            assertThat(dlqConfig.get("request.timeout.ms"))
                    .as("request.timeout.ms should be inherited from main channel (10000), not external config (88888)")
                    .isEqualTo("10000");
            assertThat(dlqConfig.get("retries"))
                    .as("retries should be inherited from main channel (5), not external config (99)")
                    .isEqualTo("5");
        });

        assertThat(isAlive()).isTrue();
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDelayedRetryTopicDLQProducerConfigInheritance() {
        addBeans(KafkaDelayedRetryTopic.Factory.class);
        String retryTopic = getRetryTopic(topic, 2000);
        String dlTopic = topic + "-dlq";

        // Test that the DLQ producer (used by delayed retry) inherits config from main channel
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "delayed-retry-topic");
        config.put("delayed-retry-topic.topics", retryTopic);
        config.put("delayed-retry-topic.max-retries", "1"); // Fail fast to DLQ
        config.put("dead-letter-queue.topic", dlTopic);

        // Set producer configs on the main channel - these should be inherited by DLQ producer
        config.put("compression.type", "gzip");
        config.put("acks", "all");
        config.put("linger.ms", "100");

        // Set DLQ interceptor to capture config
        config.put("dead-letter-queue.interceptor.classes", DLQConfigCapturingInterceptor.class.getName());

        // Reset captured config
        DLQConfigCapturingInterceptor.capturedConfig = null;

        MyReceiverBean bean = runApplication(config, MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);

        // Verify that DLQ producer inherited the configs from main channel
        await().untilAsserted(() -> {
            Map<String, ?> dlqConfig = DLQConfigCapturingInterceptor.capturedConfig;
            assertThat(dlqConfig).isNotNull();
            assertThat(dlqConfig.get("compression.type")).isEqualTo("gzip");
            assertThat(dlqConfig.get("acks")).isEqualTo("all");
            assertThat(dlqConfig.get("linger.ms")).isEqualTo("100");
        });

        assertThat(isAlive()).isTrue();
        assertThat(bean.consumers()).isEqualTo(2L); // Main consumer + delayed retry consumer
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDelayedRetryTopicConsumerConfigInheritance() {
        addBeans(KafkaDelayedRetryTopic.Factory.class);
        String retryTopic = getRetryTopic(topic, 2000);

        // Test that delayed retry topic consumer is created with correct config
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "delayed-retry-topic");
        config.put("delayed-retry-topic.topics", retryTopic);
        config.put("dead-letter-queue.topic", topic + "-dlq");

        // Set consumer configs that should be inherited by retry consumer
        config.put("max.poll.records", "500");
        config.put("session.timeout.ms", "10000");

        // Override consumer-specific config for delayed retry topic
        config.put("delayed-retry-topic.consumer.max.poll.records", "100");
        config.put("delayed-retry-topic.consumer.interceptor.classes", DRTConsumerConfigCapturingInterceptor.class.getName());

        MyReceiverBean bean = runApplication(config, MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        // Wait for messages to be processed (including retries)
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);

        // Verify that the delayed retry consumer was created
        assertThat(bean.consumers()).isEqualTo(2L); // Main consumer + delayed retry consumer

        await().untilAsserted(() -> {
            Map<String, ?> dlqConfig = DRTConsumerConfigCapturingInterceptor.capturedConfig;
            assertThat(dlqConfig).isNotNull();
            assertThat(dlqConfig.get("max.poll.records")).isEqualTo("100");
            assertThat(dlqConfig.get("session.timeout.ms")).isEqualTo("10000");
        });

        // Verify some messages were retried (sent to retry topic)
        ConsumerTask<String, Integer> retryRecords = companion.consumeIntegers().fromTopics(retryTopic);
        await().pollDelay(1, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS)
                .until(() -> retryRecords.getRecords().size() > 0);

        assertThat(isAlive()).isTrue();
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDelayedRetryStrategy() {
        addBeans(KafkaDelayedRetryTopic.Factory.class, MyObservationCollector.class);
        List<String> delayedRetryTopics = List.of(getRetryTopic(topic, 2000), getRetryTopic(topic, 4000));
        MyReceiverBean bean = runApplication(getDelayedRetryConfig(topic, delayedRetryTopics), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bean.list())
                        .hasSizeGreaterThanOrEqualTo(16)
                        .containsOnlyOnce(0, 1, 2, 4, 5, 7, 8)
                        .contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .fromTopics(delayedRetryTopics.toArray(String[]::new));

        await().untilAsserted(() -> assertThat(records.getRecords()).hasSizeGreaterThanOrEqualTo(6));
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isIn(delayedRetryTopics);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_OFFSET).value())).isNotNull().isIn("3", "6", "9");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_ORIGINAL_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_COUNT)).isNotNull();
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(2L);
        assertThat(bean.producers()).isEqualTo(1);

        MyObservationCollector collector = get(MyObservationCollector.class);
        await().untilAsserted(() -> assertThat(collector.observed()).hasSize(9)
                .allSatisfy(MessageObservation::isDone));
    }

    @Test
    public void testDelayedRetryStrategyMultiplePartitions() {
        addBeans(KafkaDelayedRetryTopic.Factory.class);
        String retryTopic1 = getRetryTopic(topic, 1000);
        String retryTopic2 = getRetryTopic(topic, 2000);
        companion.topics().create(Map.of(topic, 3, retryTopic1, 3, retryTopic2, 3, topic + "-dlq", 3));

        List<String> delayedRetryTopics = List.of(retryTopic1, retryTopic2);
        MyReceiverBean bean = runApplication(getDelayedRetryConfig(topic, delayedRetryTopics), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, (i + 1) % 3, "k" + i, i), 10);

        await().atMost(2, TimeUnit.MINUTES)
                .untilAsserted(() -> assertThat(bean.list())
                        .hasSizeGreaterThanOrEqualTo(16)
                        .containsOnlyOnce(0, 1, 2, 4, 5, 7, 8)
                        .contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .fromTopics(delayedRetryTopics.toArray(String[]::new));

        await().untilAsserted(() -> assertThat(records.getRecords()).hasSize(6));

        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isIn(delayedRetryTopics);
            assertThat(r.partition()).isEqualTo(1);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_PARTITION).value())).isEqualTo("1");
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_OFFSET).value())).isNotNull().isIn("1", "2", "3");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_ORIGINAL_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_COUNT)).isNotNull();
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(2L);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDelayedRetryStrategyWithSingleTopic() {
        addBeans(KafkaDelayedRetryTopic.Factory.class);
        String retryTopic = getRetryTopic(topic, 2000);
        MyReceiverBean bean = runApplication(getDelayedRetryConfigMaxRetries(topic, retryTopic, 2), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bean.list())
                        .hasSize(16)
                        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 3, 6, 9, 3, 6, 9));

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(retryTopic);
        await().untilAsserted(() -> assertThat(records.getRecords()).hasSize(6));
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isIn(retryTopic);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_OFFSET).value())).isNotNull().isIn("3", "6", "9");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_ORIGINAL_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_COUNT)).isNotNull();
        });

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(2L);
        assertThat(bean.producers()).isEqualTo(1);
    }

    @Test
    public void testDelayedRetryStrategyWithTimeout() {
        addBeans(KafkaDelayedRetryTopic.Factory.class);
        List<String> retryTopics = List.of(getRetryTopic(topic, 1000), getRetryTopic(topic, 10000));
        MyReceiverBean bean = runApplication(getDelayedRetryConfigTimeout(topic, retryTopics, 3000), MyReceiverBean.class);
        await().until(this::isReady);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 10);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(bean.list())
                        .hasSizeGreaterThanOrEqualTo(13)
                        .containsOnlyOnce(0, 1, 2, 4, 5, 7, 8)
                        .contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));

        ConsumerTask<String, Integer> records = companion.consumeIntegers()
                .fromTopics(retryTopics.toArray(String[]::new));
        await().untilAsserted(() -> assertThat(records.getRecords()).hasSize(3));
        assertThat(records.getRecords()).allSatisfy(r -> {
            assertThat(r.topic()).isIn(retryTopics);
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_CAUSE_CLASS_NAME)).isNull();
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_PARTITION).value())).isEqualTo("0");
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_TOPIC).value())).isEqualTo(topic);
            assertThat(new String(r.headers().lastHeader(DELAYED_RETRY_OFFSET).value())).isNotNull().isIn("3", "6", "9");
            assertThat(r.headers().lastHeader(DELAYED_RETRY_ORIGINAL_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_FIRST_PROCESSING_TIMESTAMP)).isNotNull();
            assertThat(r.headers().lastHeader(DELAYED_RETRY_COUNT)).isNotNull();
        });

        ConsumerTask<String, Integer> dlq = companion.consumeIntegers().fromTopics(topic + "-dlq", 3)
                .awaitCompletion();
        await().untilAsserted(() -> assertThat(dlq.getRecords()).hasSize(3));

        assertThat(isAlive()).isTrue();

        assertThat(bean.consumers()).isEqualTo(2L);
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

    private KafkaMapBasedConfig getDeadLetterQueueConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
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

    private KafkaMapBasedConfig getDelayedRetryConfig(String topic, List<String> topics) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "delayed-retry-topic");
        config.put("dead-letter-queue.topic", topic + "-dlq");
        config.put("delayed-retry-topic.topics", String.join(",", topics));

        return config;
    }

    private KafkaMapBasedConfig getDelayedRetryConfigMaxRetries(String topic, String topics, int maxRetries) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "delayed-retry-topic");
        config.put("delayed-retry-topic.topics", topics);
        config.put("delayed-retry-topic.max-retries", maxRetries);

        return config;
    }

    private KafkaMapBasedConfig getDelayedRetryConfigTimeout(String topic, List<String> topics, long timeout) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("topic", topic);
        config.put("group.id", UUID.randomUUID().toString());
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "delayed-retry-topic");
        config.put("dead-letter-queue.topic", topic + "-dlq");
        config.put("delayed-retry-topic.topics", String.join(",", topics));
        config.put("delayed-retry-topic.timeout", timeout);

        return config;
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private final List<Integer> received = new CopyOnWriteArrayList<>();

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

    public static class DLQConfigCapturingInterceptor<K, V> implements ProducerInterceptor<K, V> {
        public static volatile Map<String, ?> capturedConfig = null;

        @Override
        public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {
            capturedConfig = configs;
        }
    }

    public static class DRTConsumerConfigCapturingInterceptor<K, V> implements ConsumerInterceptor<K, V> {
        public static volatile Map<String, ?> capturedConfig = null;

        @Override
        public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
            return null;
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {
            capturedConfig = configs;
        }
    }

    @ApplicationScoped
    public static class ExternalKafkaConfigProvider {

        @Produces
        @ApplicationScoped
        @Identifier("default-kafka-broker")
        public Map<String, Object> createExternalKafkaConfig() {
            Map<String, Object> properties = new HashMap<>();
            // These values should NOT be used by DLQ - it should inherit from main channel instead
            properties.put("max.block.ms", "99999");
            properties.put("request.timeout.ms", "88888");
            properties.put("retries", "99");
            return properties;
        }
    }
}
