package io.smallrye.reactive.messaging.kafka.perf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@Tag(TestTags.PERFORMANCE)
public class PerformanceConsumerTest extends KafkaCompanionTestBase {

    public static final int TIMEOUT_IN_SECONDS = 400;
    public static final int COUNT = 10_000;

    public static String topic = UUID.randomUUID().toString();
    private static ArrayList<String> expected;

    @BeforeAll
    static void insertRecords() {
        expected = new ArrayList<>();
        companion.produceStrings().usingGenerator(i -> {
            expected.add(Integer.toString(i));
            return new ProducerRecord<>(topic, "key", Long.toString(i));
        }, COUNT).awaitCompletion(Duration.ofMinutes(2));
    }

    private MapBasedConfig commonConfig() {
        return commonConfig("throttled");
    }

    private MapBasedConfig commonConfig(String commitStrategy) {
        return kafkaConfig("mp.messaging.incoming.data")
                .put("topic", topic)
                .put("cloud-events", false)
                .put("pause-if-no-requests", false)
                .put("commit-strategy", commitStrategy)
                .put("auto.offset.reset", "earliest")
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName());
    }

    @Test
    // too long - ~ 1.29 minutes
    @Tag(TestTags.SLOW)
    public void testWithPostAckLatest() {
        // To speed up a bit this test we reduce the polling timeout, the 1 second by default means that the commit
        // are all delayed by 1 second. So we set the poll-timeout to 10ms

        MyConsumerUsingPostAck application = runApplication(commonConfig("latest")
                .with("poll-timeout", 5),
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
        MyConsumerUsingPostAck application = runApplication(commonConfig(),
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
        MyConsumerUsingNoAck application = runApplication(commonConfig()
                .with("enable.auto.commit", true),
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
        MyConsumerUsingPostAck application = runApplication(commonConfig()
                .with("enable.auto.commit", true),
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
        MyConsumerUsingPostAck application = runApplication(commonConfig()
                .with("pattern", true)
                .with("auto.commit.interval.ms", 1000)
                .with("metadata.max.age.ms", 30000)
                .with("enable.auto.commit", true),
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
        public void consume(String message) {
            list.add(message);
            count.increment();
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
        public void consume(String message) {
            list.add(message);
            count.increment();
        }

        public long getCount() {
            return count.longValue();
        }

        public List<String> get() {
            return list;
        }
    }

}
