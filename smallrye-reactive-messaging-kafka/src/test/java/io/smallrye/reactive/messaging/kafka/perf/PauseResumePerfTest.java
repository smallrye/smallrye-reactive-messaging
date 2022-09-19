package io.smallrye.reactive.messaging.kafka.perf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.TestTags;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.PerfTestUtils;

@Tag(TestTags.PERFORMANCE)
@Tag(TestTags.SLOW)
public class PauseResumePerfTest extends KafkaCompanionTestBase {

    public static final int TIMEOUT_IN_SECONDS = 400;
    public static final int COUNT = 50_000;

    public static String topic = UUID.randomUUID().toString();
    private static ArrayList<String> expected;

    @BeforeAll
    static void insertRecords() {
        expected = new ArrayList<>();
        companion.produceStrings().usingGenerator(i -> {
            expected.add(Long.toString(i));
            return new ProducerRecord<>(topic, "key", Long.toString(i));
        }, COUNT).awaitCompletion(Duration.ofMinutes(5));
    }

    private KafkaMapBasedConfig commonConfig() {
        return kafkaConfig("mp.messaging.incoming.data")
                .put("topic", topic)
                .put("cloud-events", false)
                .put("commit-strategy", "throttled")
                .put("auto.offset.reset", "earliest")
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName());
    }

    @Test
    public void test_noop_consumer() {
        NoopConsumer application = runApplication(commonConfig().put("pause-if-no-requests", false),
                NoopConsumer.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("No-op consumer / No pause/resume - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void test_noop_consumer_pause_resume() {
        NoopConsumer application = runApplication(commonConfig().put("pause-if-no-requests", true),
                NoopConsumer.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("No-op consumer / pause/resume - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void test_hard_working_consumer() {
        HardWorkingConsumerWithAck application = runApplication(commonConfig()
                .put("pause-if-no-requests", false),
                HardWorkingConsumerWithAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Blocking consumer / No pause/resume - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void test_hard_working_consumer_pause_resume() {
        HardWorkingConsumerWithAck application = runApplication(commonConfig()
                .put("pause-if-no-requests", true),
                HardWorkingConsumerWithAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Blocking consumer / pause/resume - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void test_hard_working_consumer_without_ack() {
        HardWorkingConsumerWithoutAck application = runApplication(commonConfig()
                .put("enable.auto.commit", true)
                .put("pause-if-no-requests", false),
                HardWorkingConsumerWithoutAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Blocking consumer without ack / No pause/resume - Estimate: " + (end - start) + " ms");
    }

    @Test
    public void test_hard_working_consumer_without_ack_pause_resume() {
        HardWorkingConsumerWithoutAck application = runApplication(commonConfig()
                .put("enable.auto.commit", true)
                .put("pause-if-no-requests", true),
                HardWorkingConsumerWithoutAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyElementsOf(expected);

        System.out.println("Blocking consumer without ack / pause/resume - Estimate: " + (end - start) + " ms");
    }

    @ApplicationScoped
    public static class NoopConsumer {

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
    public static class HardWorkingConsumerWithAck {

        LongAdder count = new LongAdder();
        List<String> list = new ArrayList<>();

        @Incoming("data")
        @Blocking
        public CompletionStage<Void> consume(Message<String> message) {
            PerfTestUtils.consumeCPU(1_000_000);
            list.add(message.getPayload());
            count.increment();
            return message.ack();
        }

        public List<String> get() {
            return list;
        }

        public long getCount() {
            return count.longValue();
        }
    }

    @ApplicationScoped
    public static class HardWorkingConsumerWithoutAck {

        LongAdder count = new LongAdder();
        List<String> list = new ArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(String message) {
            PerfTestUtils.consumeCPU(1_000_000);
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

}
