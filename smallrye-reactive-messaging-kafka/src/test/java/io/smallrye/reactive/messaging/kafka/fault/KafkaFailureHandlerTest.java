package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

public class KafkaFailureHandlerTest extends KafkaTestBase {

    @Test
    public void testFailStrategy() {
        MyReceiverBean bean = runApplication(getFailConfig("fail"), MyReceiverBean.class);

        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("fail", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        await().until(() -> !isAlive());
    }

    @Test
    public void testFailStrategyWithPayload() {
        MyReceiverBeanUsingPayload bean = runApplication(getFailConfig("fail-payload"),
                MyReceiverBeanUsingPayload.class);

        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("fail-payload", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        await().until(() -> !isAlive());
    }

    @Test
    public void testIgnoreStrategy() {
        MyReceiverBean bean = runApplication(getIgnoreConfig("ignore"), MyReceiverBean.class);
        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("ignore", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testIgnoreStrategyWithPayload() {
        MyReceiverBean bean = runApplication(getIgnoreConfig("ignore-payload"), MyReceiverBean.class);
        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("ignore-payload", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithDefaultTopic() {
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("dead-letter-topic-kafka"), records::add);

        MyReceiverBean bean = runApplication(getDeadLetterQueueConfig(), MyReceiverBean.class);
        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("dead-letter-default", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.size() == 3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("dead-letter-topic-kafka");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader("dead-letter-reason").value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader("dead-letter-cause")).isNull();
        });

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomTopicAndMethodUsingPayload() {
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("dead-letter-topic-kafka-payload"), records::add);

        MyReceiverBeanUsingPayload bean = runApplication(
                getDeadLetterQueueWithCustomConfig("dq-payload", "dead-letter-topic-kafka-payload"),
                MyReceiverBeanUsingPayload.class);
        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("dq-payload", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.size() == 3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("dead-letter-topic-kafka-payload");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader("dead-letter-reason").value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader("dead-letter-cause")).isNull();
        });

        assertThat(isAlive()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomConfig() {
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("missed"), records::add);

        MyReceiverBean bean = runApplication(getDeadLetterQueueWithCustomConfig("dead-letter-custom", "missed"),
                MyReceiverBean.class);
        await().until(this::isReady);

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("dead-letter-custom", counter.getAndIncrement()))).start();

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.size() == 3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("missed");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader("dead-letter-reason").value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader("dead-letter-cause")).isNull();
        });

        assertThat(isAlive()).isTrue();
    }

    private MapBasedConfig getFailConfig(String topic) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.kafka");
        builder.put("group.id", "my-group");
        builder.put("topic", topic);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("failure-strategy", "fail");

        return builder.build();
    }

    private MapBasedConfig getIgnoreConfig(String topic) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.kafka");
        builder.put("topic", topic);
        builder.put("group.id", "my-group");
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("failure-strategy", "ignore");

        return builder.build();
    }

    private MapBasedConfig getDeadLetterQueueConfig() {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.kafka");
        builder.put("topic", "dead-letter-default");
        builder.put("group.id", "my-group");
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("failure-strategy", "dead-letter-queue");

        return builder.build();
    }

    private MapBasedConfig getDeadLetterQueueWithCustomConfig(String topic, String dq) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.kafka");
        builder.put("group.id", "my-group");
        builder.put("topic", topic);
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("enable.auto.commit", "false");
        builder.put("auto.offset.reset", "earliest");
        builder.put("failure-strategy", "dead-letter-queue");
        builder.put("dead-letter-queue.topic", dq);
        builder.put("dead-letter-queue.key.serializer", IntegerSerializer.class.getName());
        builder.put("dead-letter-queue.value.serializer", IntegerSerializer.class.getName());

        return builder.build();
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private final List<Integer> received = new ArrayList<>();

        @Incoming("kafka")
        public CompletionStage<Void> process(KafkaRecord<String, Integer> record) {
            Integer payload = record.getPayload();
            received.add(payload);
            if (payload != 0 && payload % 3 == 0) {
                return record.nack(new IllegalArgumentException("nack 3 - " + payload));
            }
            return record.ack();
        }

        public List<Integer> list() {
            return received;
        }

    }

    @ApplicationScoped
    public static class MyReceiverBeanUsingPayload {
        private final List<Integer> received = new ArrayList<>();

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

    }
}
