package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class OffsetsTest extends KafkaCompanionTestBase {

    @Test
    void testOffsetReset() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "" + i), 500)
                .awaitCompletion();

        String groupId = UUID.randomUUID().toString();
        companion.consumeStrings()
                .withAutoCommit()
                .withGroupId(groupId)
                .fromTopics(topic, 500)
                .awaitCompletion();

        await().untilAsserted(() -> assertThat(companion.consumerGroups().offsets(groupId, tp(topic, 0)))
                .isNotNull()
                .extracting(OffsetAndMetadata::offset).isEqualTo(500L));

        ConsumerBuilder<String, String> consumer = companion.consumeStrings();
        try (ConsumerTask<String, String> records = consumer
                .withAutoCommit()
                .withGroupId(groupId)
                .withOffsetReset(OffsetResetStrategy.LATEST)
                .fromTopics(topic)) {
            consumer.waitForAssignment().await().atMost(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(0L);
        }

        assertThat(companion.consumerGroups().offsets(groupId, tp(topic, 0)).offset()).isEqualTo(500L);

        await().untilAsserted(() -> assertThat(companion.consumerGroups().describe(groupId).members()).asList().hasSize(0));

        // reset offsets
        companion.consumerGroups().resetOffsets(groupId, tp(topic, 0));

        await().untilAsserted(
                () -> assertThat(companion.consumerGroups().offsets(groupId, tp(topic, 0)).offset()).isEqualTo(0L));
    }

    @Test
    void testListOffsets() {
        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "" + i), 10)
                .awaitCompletion();

        String groupId = UUID.randomUUID().toString();
        companion.consumeStrings()
                .withGroupId(groupId)
                .withCommitSyncWhen(r -> true)
                .fromTopics(topic, 10)
                .awaitCompletion();

        HashMap<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
        TopicPartition tp = tp(topic, 0);
        offsetSpecs.put(tp, OffsetSpec.earliest());
        ListOffsetsResult.ListOffsetsResultInfo info = companion.offsets().list(offsetSpecs).get(tp);
        assertThat(info.offset()).isEqualTo(0);

        long offset = companion.offsets().list(Collections.singletonList(tp)).get(tp).offset();
        assertThat(offset).isEqualTo(10);

        long groupOffset = companion.consumerGroups().offsets(groupId, tp(topic, 0)).offset();
        assertThat(groupOffset).isEqualTo(10);
    }
}
