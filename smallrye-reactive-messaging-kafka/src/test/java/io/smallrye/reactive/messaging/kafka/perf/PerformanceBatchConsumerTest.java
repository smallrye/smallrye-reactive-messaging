package io.smallrye.reactive.messaging.kafka.perf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

public class PerformanceBatchConsumerTest extends KafkaTestBase {

    public static final int TIMEOUT_IN_SECONDS = 400;
    public static final int COUNT = 10_000;

    public static String topic = UUID.randomUUID().toString();
    private static ArrayList<String> expected;

    @BeforeAll
    static void insertRecords() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong count = new AtomicLong();
        usage.produceStrings(COUNT, latch::countDown,
                () -> new ProducerRecord<>(topic, "key", Long.toString(count.getAndIncrement())));
        expected = new ArrayList<>();
        for (int i = 0; i < COUNT; i++) {
            expected.add(Long.toString(i));
        }
        latch.await();
    }

    @Test
    public void testWithPostAckLatest() {
        // To speed up a bit this test we reduce the polling timeout, the 1 second by default means that the commit
        // are all delayed by 1 second. So we set the poll-timeout to 10ms

        MyConsumerUsingPostAck application = runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.graceful-shutdown", false)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.pause-if-no-requests", false)
                .with("mp.messaging.incoming.data.commit-strategy", "latest")
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.poll-timeout", 5)
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.batch", true),
                MyConsumerUsingPostAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Post-Ack / Latest - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithPostAckThrottled() {
        MyConsumerUsingPostAck application = runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.graceful-shutdown", false)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.pause-if-no-requests", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.commit-strategy", "throttled")
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.batch", true),
                MyConsumerUsingPostAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Post-Ack / Throttled - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithNoAck() {
        MyConsumerUsingNoAck application = runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.graceful-shutdown", false)
                .with("mp.messaging.incoming.data.enable.auto.commit", true)
                .with("mp.messaging.incoming.data.pause-if-no-requests", false)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.batch", true),
                MyConsumerUsingNoAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Ignored acknowledgement (auto-commit, no-ack) - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithAutoCommitWithPostAck() {
        MyConsumerUsingPostAck application = runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.graceful-shutdown", false)
                .with("mp.messaging.incoming.data.enable.auto.commit", true)
                .with("mp.messaging.incoming.data.pause-if-no-requests", false)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.batch", true),
                MyConsumerUsingPostAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();
        assertThat(application.get()).containsExactlyElementsOf(expected);
        System.out.println("Ignored acknowledgement (auto-commit, post-ack) - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithIgnoreAck() {
        MyConsumerUsingPostAck application = runApplication(new KafkaMapBasedConfig()
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.graceful-shutdown", false)
                .with("mp.messaging.incoming.data.pause-if-no-requests", false)
                .with("mp.messaging.incoming.data.pattern", true)
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.auto.commit.interval.ms", 1000)
                .with("mp.messaging.incoming.data.metadata.max.age.ms", 30000)
                .with("mp.messaging.incoming.data.enable.auto.commit", true)
                .with("mp.messaging.incoming.data.batch", true),
                MyConsumerUsingPostAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();
        assertThat(application.get()).containsExactlyElementsOf(expected);
        System.out.println("Ignore with Auto-Commit - Estimate: " + (end - start) + " ms");
    }

    @ApplicationScoped
    public static class MyConsumerUsingPostAck {

        LongAdder count = new LongAdder();
        List<String> list = new ArrayList<>();

        @Incoming("data")
        public void consume(List<String> message) {
            list.addAll(message);
            count.add(message.size());
        }

        public List<String> get() {
            return list;
        }

        public long getCount() {
            return count.longValue();
        }
    }

    @ApplicationScoped
    public static class MyConsumerUsingNoAck {

        LongAdder count = new LongAdder();
        List<String> list = new ArrayList<>();

        @Incoming("data")
        @Acknowledgment(Acknowledgment.Strategy.NONE)
        public void consume(List<String> message) {
            list.addAll(message);
            count.add(message.size());
        }

        public long getCount() {
            return count.longValue();
        }

        public List<String> get() {
            return list;
        }
    }

}
