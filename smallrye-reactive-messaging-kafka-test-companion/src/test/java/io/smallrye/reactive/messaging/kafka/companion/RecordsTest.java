package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class RecordsTest extends KafkaCompanionTestBase {

    @Test
    void testDeleteRecords() throws InterruptedException {
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, i), 100)
                .awaitCompletion();

        long offset = companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset();
        assertThat(companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset()).isEqualTo(100L);

        companion.deleteRecords(tp(topic, 0), offset);

        ConsumerTask<String, Integer> records = companion.consumeIntegers().fromTopics(topic, 100);
        Thread.sleep(2000);
        await().until(() -> records.count() == 0);

        assertThat(companion.offsets().get(tp(topic, 0), OffsetSpec.latest()).offset()).isEqualTo(100L);
    }
}
