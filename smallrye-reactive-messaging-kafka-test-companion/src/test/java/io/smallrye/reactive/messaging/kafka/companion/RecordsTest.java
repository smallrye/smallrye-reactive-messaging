package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class RecordsTest extends KafkaCompanionTestBase {

    @Test
    void testDeleteRecords() {
        companion.produceIntegers().withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 100)
                .awaitCompletion();

        long offset = companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset();
        assertThat(companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset()).isEqualTo(100L);

        companion.deleteRecords(tp(topic, 0), offset);

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, 100);
        records.awaitNoRecords(Duration.ofSeconds(2));
        // Deleting records doesn't clear offsets
        assertThat(companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset()).isEqualTo(100L);
    }

    @Test
    void testClearRecords() {
        companion.produceIntegers()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 100)
                .awaitCompletion();

        assertThat(companion.consumeIntegers().fromTopics(topic, 100).awaitCompletion().count()).isEqualTo(100L);

        assertThat(companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset()).isEqualTo(100L);

        companion.topics().clear(topic);

        companion.consumeIntegers().fromTopics(topic).awaitNoRecords(Duration.ofSeconds(2));
        // Deleting records doesn't clear offsets
        assertThat(companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset()).isEqualTo(100L);
    }

    @Test
    void testClearTopicThatDoesNotExist() {
        assertThrows(UnknownTopicOrPartitionException.class, () -> {
            companion.topics().clear("non-existent-topic");
        });
        companion.topics().clearIfExists("non-existent-topic"); // Should not fail
    }
}
