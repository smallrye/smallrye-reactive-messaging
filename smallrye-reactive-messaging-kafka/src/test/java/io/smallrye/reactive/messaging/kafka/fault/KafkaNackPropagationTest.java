package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.smallrye.config.SmallRyeConfigProviderResolver;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.KafkaUsage;
import io.smallrye.reactive.messaging.kafka.MapBasedConfig;

public class KafkaNackPropagationTest extends KafkaTestBase {

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
    public void testDoubleNackPropagation() {
        KafkaUsage usage = new KafkaUsage();
        List<Integer> messages = new CopyOnWriteArrayList<>();
        usage.consumeIntegers("double-topic", 9, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(value));

        addConfig(getDoubleNackConfig());
        container = baseWeld().addBeanClass(DoubleNackBean.class).initialize();

        DoubleNackBean bean = container.getBeanManager().createInstance().select(DoubleNackBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 9);
        assertThat(bean.list()).containsExactly(0, 1, 2, 3, 4, 5, 7, 8, 9);

        assertThat(bean.m2Nack()).isEqualTo(1);
        assertThat(bean.m1Nack()).isEqualTo(1);
        assertThat(bean.nackedException()).isNotNull().isInstanceOf(IllegalArgumentException.class);

        await().until(() -> messages.size() == 9);
        assertThat(messages).containsExactly(1, 2, 3, 4, 5, 6, 8, 9, 10);
    }

    @Test
    public void testNackPropagation() {
        String topic = UUID.randomUUID().toString();
        KafkaUsage usage = new KafkaUsage();
        List<Integer> messages = new CopyOnWriteArrayList<>();
        usage.consumeIntegers(topic, 9, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(value));

        addConfig(getPassedNackConfig(topic));
        container = baseWeld().addBeanClass(PassedNackBean.class).initialize();

        PassedNackBean bean = container.getBeanManager().createInstance().select(PassedNackBean.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 9);
        assertThat(bean.list()).containsExactly(0, 1, 2, 4, 5, 6, 7, 8, 9);

        assertThat(bean.m1Nack()).isEqualTo(1);
        assertThat(bean.nackedException()).isNotNull().isInstanceOf(IllegalArgumentException.class);

        await().until(() -> messages.size() == 9);
        assertThat(messages).containsExactly(1, 2, 3, 5, 6, 7, 8, 9, 10);
    }

    private MapBasedConfig getDoubleNackConfig() {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", IntegerSerializer.class.getName());
        config.put(prefix + "topic", "double-topic");
        return new MapBasedConfig(config);
    }

    private MapBasedConfig getPassedNackConfig(String topic) {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", IntegerSerializer.class.getName());
        config.put(prefix + "topic", topic);
        return new MapBasedConfig(config);
    }

    @ApplicationScoped
    public static class DoubleNackBean {
        private final List<Integer> received = new ArrayList<>();
        private final AtomicInteger m1Nack = new AtomicInteger();
        private final AtomicInteger m2Nack = new AtomicInteger();
        private final AtomicReference<Throwable> nackedException = new AtomicReference<>();

        @Outgoing("source")
        public Flowable<Message<Integer>> producer() {
            return Flowable.range(0, 10)
                    .map(val -> KafkaRecord.of("1", val)
                            .withNack(cause -> {
                                assertThat(cause).isNotNull();
                                m1Nack.incrementAndGet();
                                nackedException.set(cause);
                                return CompletableFuture.completedFuture(null);
                            }));
        }

        @Incoming("source")
        @Outgoing("output")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.from(input)
                    .withNack(cause -> {
                        assertThat(cause).isNotNull();
                        m2Nack.incrementAndGet();
                        input.nack(cause);
                        return CompletableFuture.completedFuture(null);
                    });
        }

        @Incoming("output")
        @Outgoing("kafka")
        public Message<Integer> handle(Message<Integer> record) {
            if (record.getPayload().equals(6)) {
                record.nack(new IllegalArgumentException()).toCompletableFuture().join();
                return null;
            } else {
                received.add(record.getPayload());
                record.ack().toCompletableFuture().join();
            }
            return record.withPayload(record.getPayload() + 1);
        }

        public List<Integer> list() {
            return received;
        }

        public int m1Nack() {
            return m1Nack.get();
        }

        public int m2Nack() {
            return m2Nack.get();
        }

        public Throwable nackedException() {
            return nackedException.get();
        }
    }

    @ApplicationScoped
    public static class PassedNackBean {
        private List<Integer> received = new ArrayList<>();
        private AtomicInteger m1Nack = new AtomicInteger();
        private AtomicReference<Throwable> nackedException = new AtomicReference<>();

        @Outgoing("source")
        public Flowable<Message<Integer>> producer() {
            return Flowable.range(0, 10)
                    .map(val -> KafkaRecord.of("1", val)
                            .withNack(cause -> {
                                assertThat(cause).isNotNull();
                                m1Nack.incrementAndGet();
                                nackedException.set(cause);
                                return CompletableFuture.completedFuture(null);
                            }));
        }

        @Incoming("source")
        @Outgoing("output")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.from(input);
        }

        @Incoming("output")
        @Outgoing("kafka")
        public Message<Integer> handle(Message<Integer> record) {
            if (record.getPayload().equals(3)) {
                record.nack(new IllegalArgumentException()).toCompletableFuture().join();
                return null;
            } else {
                received.add(record.getPayload());
                record.ack().toCompletableFuture().join();
            }
            return record.withPayload(record.getPayload() + 1);
        }

        public List<Integer> list() {
            return received;
        }

        public int m1Nack() {
            return m1Nack.get();
        }

        public Throwable nackedException() {
            return nackedException.get();
        }
    }

}
