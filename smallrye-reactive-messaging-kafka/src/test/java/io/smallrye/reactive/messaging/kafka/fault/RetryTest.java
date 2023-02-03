package io.smallrye.reactive.messaging.kafka.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.NonBlocking;
import io.smallrye.faulttolerance.FaultToleranceExtension;
import io.smallrye.metrics.MetricRegistries;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class RetryTest extends KafkaCompanionTestBase {

    @Test
    public void testRetry() {
        weld.addExtensions(FaultToleranceExtension.class);
        weld.addBeanClass(MetricRegistries.class);

        MyHalfBrokenConsumer bean = runApplication(getConfig(topic), MyHalfBrokenConsumer.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i)), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.received().size() >= 10);
        assertThat(bean.received()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(bean.acks()).isEqualTo(10);
        assertThat(bean.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryProcessor() {
        weld.addExtensions(FaultToleranceExtension.class);
        weld.addBeanClass(MetricRegistries.class);

        MyHalfBrokenProcessor bean = runApplication(getConfig(topic), MyHalfBrokenProcessor.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i)), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.received().size() >= 10);
        assertThat(bean.received()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(bean.acks()).isEqualTo(10);
        assertThat(bean.nacks()).isEqualTo(0);
    }

    @Test
    public void testRetryUni() {
        weld.addExtensions(FaultToleranceExtension.class);
        weld.addBeanClass(MetricRegistries.class);

        MyHalfBrokenConsumerUni bean = runApplication(getConfig(topic), MyHalfBrokenConsumerUni.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i)), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.received().size() >= 10);
        assertThat(bean.received()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(bean.acks()).isEqualTo(10);
        assertThat(bean.nacks()).isEqualTo(0);
    }

    @Test
    public void testFailingRetries() {
        weld.addExtensions(FaultToleranceExtension.class);
        weld.addBeanClass(MetricRegistries.class);

        MyBrokenConsumer bean = runApplication(getConfig(topic), MyBrokenConsumer.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i)), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.nacks() == 10);
        assertThat(bean.acks()).isEqualTo(0);
    }

    @Test
    public void testFailingRetriesProcessor() {
        weld.addExtensions(FaultToleranceExtension.class);
        weld.addBeanClass(MetricRegistries.class);

        MyBrokenProcessor bean = runApplication(getConfig(topic), MyBrokenProcessor.class);
        await().until(this::isReady);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, Integer.toString(i)), 10);

        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.nacks() == 10);
        assertThat(bean.acks()).isEqualTo(0);
    }

    private KafkaMapBasedConfig getConfig(String topic) {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka");
        config.put("group.id", UUID.randomUUID().toString());
        config.put("topic", topic);
        config.put("value.deserializer", StringDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        config.put("failure-strategy", "ignore");

        return config;
    }

    @ApplicationScoped
    public static class MyHalfBrokenConsumer {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger nacks = new AtomicInteger();

        private int attempt = 0;

        @Incoming("kafka")
        @Outgoing("sink")
        public Message<String> process(Message<String> s) {
            return s.withAck(() -> {
                acks.incrementAndGet();
                return s.ack();
            }).withNack(t -> {
                nacks.incrementAndGet();
                return s.nack(t);
            });
        }

        @Incoming("sink")
        @Retry(delay = 10, maxRetries = 5)
        public void consume(String v) {
            if (attempt < 4) {
                if (new Random().nextBoolean()) {
                    attempt++;
                    throw new IllegalArgumentException("boom");
                }
            }
            received.add(v);
            attempt = 0;
        }

        public List<String> received() {
            return received;
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }

    }

    @ApplicationScoped
    public static class MyHalfBrokenProcessor {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger nacks = new AtomicInteger();

        private int attempt = 0;

        @Incoming("kafka")
        @Outgoing("sink")
        public Message<String> process(Message<String> s) {
            return s.withAck(() -> {
                acks.incrementAndGet();
                return s.ack();
            }).withNack(t -> {
                nacks.incrementAndGet();
                return s.nack(t);
            });
        }

        @Incoming("sink")
        @Outgoing("a")
        @Retry(delay = 10, maxRetries = 5)
        public String process(String v) {
            if (attempt < 4) {
                if (new Random().nextBoolean()) {
                    attempt++;
                    throw new IllegalArgumentException("boom");
                }
            }
            attempt = 0;
            return v;
        }

        @Incoming("a")
        public void consume(String v) {
            received.add(v);
        }

        public List<String> received() {
            return received;
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }

    }

    @ApplicationScoped
    public static class MyHalfBrokenConsumerUni {

        private final List<String> received = new CopyOnWriteArrayList<>();
        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger nacks = new AtomicInteger();

        private int attempt = 0;

        @Incoming("kafka")
        @Outgoing("sink")
        public Message<String> process(Message<String> s) {
            return s.withAck(() -> {
                acks.incrementAndGet();
                return s.ack();
            }).withNack(t -> {
                nacks.incrementAndGet();
                return s.nack(t);
            });
        }

        @Incoming("sink")
        @Retry(delay = 10, maxRetries = 5)
        @NonBlocking
        public Uni<Void> consume(String v) {
            if (attempt < 4) {
                if (new Random().nextBoolean()) {
                    attempt++;
                    throw new IllegalArgumentException("boom");
                }
            }
            received.add(v);
            attempt = 0;
            return Uni.createFrom().voidItem();
        }

        public List<String> received() {
            return received;
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }

    }

    @ApplicationScoped
    public static class MyBrokenConsumer {

        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger nacks = new AtomicInteger();

        @Incoming("kafka")
        @Outgoing("sink")
        public Message<String> process(Message<String> s) {
            return s.withAck(() -> {
                acks.incrementAndGet();
                return s.ack();
            }).withNack(t -> {
                nacks.incrementAndGet();
                return s.nack(t);
            });
        }

        @Incoming("sink")
        @Retry(maxRetries = 5)
        public void consume(String v) {
            throw new IllegalArgumentException("boom");
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }

    }

    @ApplicationScoped
    public static class MyBrokenProcessor {

        private final AtomicInteger acks = new AtomicInteger();
        private final AtomicInteger nacks = new AtomicInteger();

        @Incoming("kafka")
        @Outgoing("a")
        public Message<String> process(Message<String> s) {
            return s.withAck(() -> {
                acks.incrementAndGet();
                return s.ack();
            }).withNack(t -> {
                nacks.incrementAndGet();
                return s.nack(t);
            });
        }

        @Incoming("a")
        @Outgoing("b")
        @Retry(maxRetries = 5)
        public String process(String v) {
            throw new IllegalArgumentException("boom");
        }

        @Incoming("b")
        public void consume(String v) {
            // not called
        }

        public int acks() {
            return acks.get();
        }

        public int nacks() {
            return nacks.get();
        }

    }
}
