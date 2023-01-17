package io.smallrye.reactive.messaging.kafka.fault;

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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
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
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_EXCEPTION_CLASS_NAME).value()))
                    .isEqualTo(IllegalArgumentException.class.getName());
            assertThat(new String(r.headers().lastHeader(DEAD_LETTER_REASON).value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE)).isNull();
            assertThat(r.headers().lastHeader(DEAD_LETTER_CAUSE_CLASS_NAME)).isNull();
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
    public void testDelayedRetryStrategy() {
        addBeans(KafkaDelayedRetryTopic.Factory.class);
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
}
