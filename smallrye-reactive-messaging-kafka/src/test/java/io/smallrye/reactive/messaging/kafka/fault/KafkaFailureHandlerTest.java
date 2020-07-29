package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
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
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.kafka.*;

public class KafkaFailureHandlerTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testFailStrategy() {
        addConfig(getFailConfig("fail"));
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("fail", counter.getAndIncrement()))).start();

        MyReceiverBean bean = container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        await().until(() -> {
            HealthReport liveness = getHealth(container).getLiveness();
            return !liveness.isOk();
        });
    }

    @Test
    public void testFailStrategyWithPayload() {
        addConfig(getFailConfig("fail-payload"));
        container = baseWeld().addBeanClass(MyReceiverBeanUsingPayload.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("fail-payload", counter.getAndIncrement()))).start();

        MyReceiverBeanUsingPayload bean = container.getBeanManager().createInstance().select(MyReceiverBeanUsingPayload.class)
                .get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);

        await().until(() -> {
            HealthReport liveness = getHealth(container).getLiveness();
            return !liveness.isOk();
        });
    }

    @Test
    public void testIgnoreStrategy() {
        addConfig(getIgnoreConfig("ignore"));
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("ignore", counter.getAndIncrement()))).start();

        MyReceiverBean bean = container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        HealthReport liveness = getHealth(container).getLiveness();
        assertThat(liveness.isOk()).isTrue();
    }

    @Test
    public void testIgnoreStrategyWithPayload() {
        addConfig(getIgnoreConfig("ignore-payload"));
        container = baseWeld().addBeanClass(MyReceiverBeanUsingPayload.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("ignore-payload", counter.getAndIncrement()))).start();

        MyReceiverBeanUsingPayload bean = container.getBeanManager().createInstance().select(MyReceiverBeanUsingPayload.class)
                .get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        HealthReport liveness = getHealth(container).getLiveness();
        assertThat(liveness.isOk()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithDefaultTopic() {
        KafkaUsage usage = new KafkaUsage();
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("dead-letter-topic-kafka"), records::add);

        addConfig(getDeadLetterQueueConfig());
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("dead-letter-default", counter.getAndIncrement()))).start();

        MyReceiverBean bean = container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.size() == 3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("dead-letter-topic-kafka");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader("dead-letter-reason").value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader("dead-letter-cause")).isNull();
        });

        HealthReport liveness = getHealth(container).getLiveness();
        assertThat(liveness.isOk()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomTopicAndMethodUsingPayload() {
        KafkaUsage usage = new KafkaUsage();
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("dead-letter-topic-kafka-payload"), records::add);

        addConfig(getDeadLetterQueueWithCustomConfig("dq-payload", "dead-letter-topic-kafka-payload"));
        container = baseWeld().addBeanClass(MyReceiverBeanUsingPayload.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("dq-payload", counter.getAndIncrement()))).start();

        MyReceiverBeanUsingPayload bean = container.getBeanManager().createInstance().select(MyReceiverBeanUsingPayload.class)
                .get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.size() == 3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("dead-letter-topic-kafka-payload");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader("dead-letter-reason").value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader("dead-letter-cause")).isNull();
        });

        HealthReport liveness = getHealth(container).getLiveness();
        assertThat(liveness.isOk()).isTrue();
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomConfig() {
        KafkaUsage usage = new KafkaUsage();
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("missed"), records::add);

        addConfig(getDeadLetterQueueWithCustomConfig("dead-letter-custom", "missed"));
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();

        await().until(() -> {
            HealthReport readiness = getHealth(container).getReadiness();
            return readiness.isOk();
        });

        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("dead-letter-custom", counter.getAndIncrement()))).start();

        MyReceiverBean bean = container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        await().atMost(2, TimeUnit.MINUTES).until(() -> records.size() == 3);
        assertThat(records).allSatisfy(r -> {
            assertThat(r.topic()).isEqualTo("missed");
            assertThat(r.value()).isIn(3, 6, 9);
            assertThat(new String(r.headers().lastHeader("dead-letter-reason").value())).startsWith("nack 3 -");
            assertThat(r.headers().lastHeader("dead-letter-cause")).isNull();
        });

        HealthReport liveness = getHealth(container).getLiveness();
        assertThat(liveness.isOk()).isTrue();
    }

    private MapBasedConfig getFailConfig(String topic) {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "topic", topic);
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "tracing-enabled", false);
        // fail is the default.

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getIgnoreConfig(String topic) {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "topic", topic);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "failure-strategy", "ignore");
        config.put(prefix + "tracing-enabled", false);

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getDeadLetterQueueConfig() {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "topic", "dead-letter-default");
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "failure-strategy", "dead-letter-queue");
        config.put(prefix + "tracing-enabled", false);

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getDeadLetterQueueWithCustomConfig(String topic, String dq) {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "topic", topic);
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "failure-strategy", "dead-letter-queue");
        config.put(prefix + "dead-letter-queue.topic", dq);
        config.put(prefix + "dead-letter-queue.key.serializer", IntegerSerializer.class.getName());
        config.put(prefix + "dead-letter-queue.value.serializer", IntegerSerializer.class.getName());
        config.put(prefix + "tracing-enabled", false);

        return new MapBasedConfig(config);
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
