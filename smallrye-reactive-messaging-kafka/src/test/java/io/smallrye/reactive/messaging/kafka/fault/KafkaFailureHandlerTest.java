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
        addConfig(getFailConfig());
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();
        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("fail", counter.getAndIncrement()))).start();

        MyReceiverBean bean = container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 4);
        // Other records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3);
    }

    @Test
    public void testIgnoreStrategy() {
        addConfig(getIgnoreConfig());
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();
        KafkaUsage usage = new KafkaUsage();
        AtomicInteger counter = new AtomicInteger();
        new Thread(() -> usage.produceIntegers(10, null,
                () -> new ProducerRecord<>("ignore", counter.getAndIncrement()))).start();

        MyReceiverBean bean = container.getBeanManager().createInstance().select(MyReceiverBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        // All records should not have been received.
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
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
    }

    @Test
    public void testDeadLetterQueueStrategyWithCustomConfig() {
        KafkaUsage usage = new KafkaUsage();
        List<ConsumerRecord<String, Integer>> records = new CopyOnWriteArrayList<>();
        String randomId = UUID.randomUUID().toString();

        usage.consume(randomId, randomId, OffsetResetStrategy.EARLIEST,
                new StringDeserializer(), new IntegerDeserializer(), () -> records.size() < 3, null, null,
                Collections.singletonList("missed"), records::add);

        addConfig(getDeadLetterQueueWithCustomConfig());
        container = baseWeld().addBeanClass(MyReceiverBean.class).initialize();
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
    }

    private MapBasedConfig getFailConfig() {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "topic", "fail");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        // fail is the default.

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getIgnoreConfig() {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "topic", "ignore");
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "failure-strategy", "ignore");

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

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getDeadLetterQueueWithCustomConfig() {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "topic", "dead-letter-custom");
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "failure-strategy", "dead-letter-queue");
        config.put(prefix + "dead-letter-queue.topic", "missed");
        config.put(prefix + "dead-letter-queue.key.serializer", IntegerSerializer.class.getName());
        config.put(prefix + "dead-letter-queue.value.serializer", IntegerSerializer.class.getName());

        return new MapBasedConfig(config);
    }

    @ApplicationScoped
    public static class MyReceiverBean {
        private List<Integer> received = new ArrayList<>();

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
}
