package kafka.companion;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.ConsumerBuilder;
import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ConsumerTest extends KafkaCompanionTestBase {

    @Test
    public void fromTopics() {
        // tag::topics[]
        companion.consumeIntegers().fromTopics("topic1", "topic2");
        // end::topics[]
    }

    @Test
    public void fromOffset() {
        // tag::offsets[]
        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition("topic1", 0), 100L);
        offsets.put(new TopicPartition("topic2", 0), 100L);
        companion.consumeIntegers().fromOffsets(offsets, Duration.ofSeconds(10));
        // end::offsets[]
    }

    @Test
    public void committed() {
        // tag::committed[]
        ConsumerBuilder<String, Integer> consumer = companion.consumeIntegers()
                .withAutoCommit();
        consumer.fromTopics("topic");
        // ...
        await().untilAsserted(consumer::waitForAssignment);
        consumer.committed(new TopicPartition("topic", 1));
        // end::committed[]
    }
}
