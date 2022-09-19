package io.smallrye.reactive.messaging.kafka.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

@Tag(TestTags.SLOW)
public class PauseResumeBatchTest extends KafkaCompanionTestBase {

    public static final int TIMEOUT_IN_SECONDS = 60;
    public static final int COUNT = 100_000;

    public static final int partitions = 3;

    public static String topic = UUID.randomUUID().toString();
    private static List<String> expected;

    @BeforeAll
    static void insertRecords() {
        expected = new ArrayList<>();
        companion.topics().createAndWait(topic, partitions);
        companion.produceStrings().usingGenerator(i -> {
            expected.add(Long.toString(i));
            return new ProducerRecord<>(topic, "k-" + i, Long.toString(i));
        }, COUNT).awaitCompletion(Duration.ofMinutes(2));
    }

    private KafkaMapBasedConfig commonConfig() {
        return kafkaConfig("mp.messaging.incoming.data")
                .put("topic", topic)
                .put("partitions", partitions)
                .put("batch", true)
                .put("cloud-events", false)
                .put("auto.offset.reset", "earliest")
                .put("pause-if-no-requests", true)
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName());
    }

    @Test
    public void testWithAutoCommitMultiplePartitions() {
        MyConsumerUsingNoAck application = runApplication(commonConfig()
                .put("enable.auto.commit", true)
                .put("max.poll.records", 100)
                .put("requests", partitions)
                .put("auto.commit.interval.ms", 200),
                MyConsumerUsingNoAck.class);
        long start = System.currentTimeMillis();
        await()
                .atMost(Duration.ofSeconds(TIMEOUT_IN_SECONDS))
                .until(() -> application.getCount() == COUNT);

        long end = System.currentTimeMillis();

        assertThat(application.get()).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ApplicationScoped
    public static class MyConsumerUsingNoAck {

        LongAdder count = new LongAdder();
        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
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
