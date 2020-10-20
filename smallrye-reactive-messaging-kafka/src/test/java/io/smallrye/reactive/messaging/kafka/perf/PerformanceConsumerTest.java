package io.smallrye.reactive.messaging.kafka.perf;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
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
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaUsage;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

public class PerformanceConsumerTest extends KafkaTestBase {

    public static final int TIMEOUT_IN_SECONDS = 120;
    public static final int COUNT = 10_000;

    public static String topic = UUID.randomUUID().toString();

    @BeforeAll
    static void insertRecords() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong count = new AtomicLong();
        KafkaUsage usage = new KafkaUsage();
        usage.produceStrings(COUNT, latch::countDown,
                () -> new ProducerRecord<>(topic, "key", Long.toString(count.getAndIncrement())));

        latch.await();
    }

    @Test
    public void testWithPostAck() {
        MyConsumerUsingPostAck application = runApplication(new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName()),
                MyConsumerUsingPostAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        System.out.println("Post-Ack / Latest - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithPostAckThrottled() {
        MyConsumerUsingPostAck application = runApplication(new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.commit-strategy", "throttled")
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName()),
                MyConsumerUsingPostAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        System.out.println("Post-Ack / Throttled - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void testWithNoAck() {
        MyConsumerUsingNoAck application = runApplication(new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.enable.auto.commit", true)
                .with("mp.messaging.incoming.data.tracing-enabled", false)
                .with("mp.messaging.incoming.data.cloud-events", false)
                .with("mp.messaging.incoming.data.bootstrap.servers", getBootstrapServers())
                .with("mp.messaging.incoming.data.auto.offset.reset", "earliest")
                .with("mp.messaging.incoming.data.value.deserializer", StringDeserializer.class.getName())
                .with("mp.messaging.incoming.data.key.deserializer", StringDeserializer.class.getName()),
                MyConsumerUsingNoAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> {
                    return application.getCount() == COUNT;
                });

        long end = System.currentTimeMillis();

        System.out.println("No Ack - Estimate: " + (end - start) + " ms");
    }

    @ApplicationScoped
    public static class MyConsumerUsingPostAck {

        LongAdder count = new LongAdder();

        @Incoming("data")
        public void consume(String message) {
            count.increment();
        }

        public long getCount() {
            return count.longValue();
        }
    }

    @ApplicationScoped
    public static class MyConsumerUsingNoAck {

        LongAdder count = new LongAdder();

        @Incoming("data")
        @Acknowledgment(Acknowledgment.Strategy.NONE)
        public void consume(String message) {
            count.increment();
        }

        public long getCount() {
            return count.longValue();
        }
    }

}
