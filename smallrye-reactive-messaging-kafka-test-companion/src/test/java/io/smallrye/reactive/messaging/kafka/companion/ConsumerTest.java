package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.record;
import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.until;
import static io.smallrye.reactive.messaging.kafka.companion.RecordQualifiers.withCallback;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ConsumerTest extends KafkaCompanionTestBase {

    @Test
    void testConsumeFromTopics() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();
        companion.produceIntegers().fromRecords(
                record(topic1, 1),
                record(topic2, 2),
                record(topic3, 3)).awaitCompletion();

        ConsumerTask<String, Integer> task = companion.consumeIntegers()
                .fromTopics(new HashSet<>(Arrays.asList(topic1, topic2, topic3)), 3)
                .awaitCompletion();

        assertThat(task.getRecords()).hasSize(3).allSatisfy(cr -> assertThat(cr.offset()).isEqualTo(0L));

        Map<TopicPartition, List<ConsumerRecord<String, Integer>>> records = task.byTopicPartition();
        assertThat(records).containsOnlyKeys(tp(topic1, 0), tp(topic2, 0), tp(topic3, 0));
    }

    @Test
    void testConsumeParallel() {
        companion.produceIntegers()
                .withConcurrency()
                .usingGenerator(i -> record(topic, i), 1000)
                .awaitCompletion(Duration.ofSeconds(10));

        AtomicInteger records = new AtomicInteger();
        try (ConsumerTask<String, Integer> task = companion.consumeIntegers()
                .fromTopics(topic, withCallback(cr -> records.incrementAndGet(), 10))) {
            await().until(() -> task.count() == 1000);
        }

        assertThat(records.get()).isEqualTo(1000);
    }

    @Test
    void testConsumeWithCallback() {
        companion.produceIntegers().usingGenerator(i -> record(topic, i), 10)
                .awaitCompletion();

        AtomicInteger records = new AtomicInteger();
        try (ConsumerTask<String, Integer> task = companion.consumeIntegers()
                .fromTopics(topic, withCallback(cr -> records.incrementAndGet()))) {
            await().until(() -> task.count() == 10);
        }

        assertThat(records.get()).isEqualTo(10);
    }

    @Test
    void testConsumeUntilPredicate() {
        companion.produceIntegers().usingGenerator(i -> record(topic, i), 10)
                .awaitCompletion();

        try (ConsumerTask<String, Integer> task = companion.consumeIntegers()
                .fromTopics(topic, until(cr -> cr.value() == 5))) {
            await().until(() -> task.count() == 5);
        }
    }

    @Test
    void testConsumeUntil() {
        companion.produceIntegers().usingGenerator(i -> record(topic, i), 10)
                .awaitCompletion();

        try (ConsumerTask<String, Integer> task = companion.consumeIntegers()
                .fromTopics(topic, until(5L, Duration.ofSeconds(100), cr -> cr.value() == 8))) {
            await().until(() -> task.count() == 5);
        }
    }

    @Test
    void testConsumeWithDeserializers() {
        companion.produce(Integer.class, String.class).fromRecords(
                record(topic, 1, "1"),
                record(topic, 2, "2"),
                record(topic, 3, "3")).awaitCompletion();

        assertThat(companion.consumeWithDeserializers(IntegerDeserializer.class, StringDeserializer.class)
                .fromTopics(topic, 3)
                .awaitCompletion().getRecords())
                .hasSize(3)
                .extracting(ConsumerRecord::key).containsExactly(1, 2, 3);

        assertThat(companion.consumeWithDeserializers(new IntegerDeserializer(), new StringDeserializer())
                .fromTopics(topic, 3)
                .awaitCompletion().getRecords())
                .hasSize(3)
                .extracting(ConsumerRecord::key).containsExactly(1, 2, 3);
    }

    @Test
    void testConsumeFromOffsets() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 100)
                .awaitCompletion();

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(topic, 0), 80L);
        ConsumerTask<String, String> records = companion.consumeStrings()
                .withGroupId("new-group")
                .fromOffsets(offsets);

        records.awaitRecords(20).stop();

        assertThat(records.count()).isEqualTo(20);
        assertThat(records.firstOffset()).isEqualTo(80L);
        assertThat(records.getLastRecord().offset()).isEqualTo(99L);
    }

    @Test
    void testConsumeFromOffsetsWithDuration() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 100)
                .awaitCompletion();

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsets.put(new TopicPartition(topic, 0), 80L);
        ConsumerTask<String, String> records = companion.consumeStrings()
                .withGroupId("new-group")
                .fromOffsets(offsets, Duration.ofSeconds(5))
                .awaitCompletion();

        assertThat(records.count()).isEqualTo(20);
        assertThat(records.firstOffset()).isEqualTo(80L);
        assertThat(records.getLastRecord().offset()).isEqualTo(99L);
    }

    @Test
    void testConsumerWithCommitAsync() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 108)
                .awaitCompletion();

        ConsumerBuilder<String, String> consumer = companion.consumeStrings()
                .withCommitAsyncWhen(cr -> cr.offset() % 10 == 0);

        consumer.fromTopics(topic, 108).awaitCompletion();

        await().untilAsserted(() -> assertThat(consumer.committed(tp(topic, 0)).offset()).isEqualTo(101L));
    }

    @Test
    void testConsumerWithCommitAsyncCallback() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 108)
                .awaitCompletion();

        List<Map<TopicPartition, OffsetAndMetadata>> commits = new CopyOnWriteArrayList<>();
        ConsumerBuilder<String, String> consumer = companion.consumeStrings()
                .withCommitAsync(cr -> {
                    if (cr.offset() % 10 == 0) {
                        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        map.put(tp(cr.topic(), cr.partition()), new OffsetAndMetadata(cr.offset()));
                        return map;
                    }
                    return Collections.emptyMap();
                }, (offsets, e) -> commits.add(offsets));

        consumer.fromTopics(topic).awaitRecords(100);

        await().untilAsserted(() -> assertThat(commits.size()).isEqualTo(11));

        await().untilAsserted(() -> {
            TopicPartition tp = tp(topic, 0);
            OffsetAndMetadata actual = consumer.committed(new TopicPartition[] { tp }).get(tp);
            assertThat(actual.offset()).isEqualTo(100L);
        });
    }

    @Test
    void testConsumerWithCommitSync() {
        companion.produceStrings()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i), 108)
                .awaitCompletion();

        ConsumerBuilder<String, String> consumer = companion.consumeStrings()
                .withCommitSyncWhen(cr -> cr.offset() % 10 == 0);

        ConsumerTask<String, String> task = consumer.fromTopics(topic, Duration.ofSeconds(5));

        await().untilAsserted(() -> assertThat(consumer.committed().get(tp(topic, 0)).offset()).isEqualTo(101L));

        task.awaitCompletion();
    }

    @Test
    void testWaitForAssignments() {
        try (ProducerTask producer = companion.produceStrings()
                .usingGenerator(i -> new ProducerRecord<>(topic, "t" + i))) {

            ConsumerBuilder<String, String> consumer = companion.consumeStrings();
            try (ConsumerTask<String, String> records = consumer.fromTopics(topic)) {
                assertThat(consumer.waitForAssignment().await().indefinitely()).hasSize(1);
                assertThat(records.awaitNextRecords(100, Duration.ofMinutes(1)).count()).isGreaterThanOrEqualTo(100);
            }
        }
    }

    @Test
    void testAssignment() {
        String topic1 = UUID.randomUUID().toString();
        String topic2 = UUID.randomUUID().toString();
        String topic3 = UUID.randomUUID().toString();
        companion.produceIntegers().fromRecords(
                record(topic1, 1),
                record(topic2, 2),
                record(topic3, 3)).awaitCompletion();

        ConsumerBuilder<String, String> consumer = companion.consumeStrings();
        try (ConsumerTask<String, String> records = consumer.fromTopics(topic1, topic2, topic3)) {
            consumer.waitForAssignment().await().indefinitely();
            assertThat(consumer.assignment()).hasSize(3);
            assertThat(consumer.currentAssignment()).hasSize(3);

            records.awaitRecords(3);
            assertThat(consumer.assignment()).hasSize(3);
            assertThat(consumer.currentAssignment()).hasSize(3);
        }
        assertThat(consumer.assignment()).hasSize(0);
        assertThat(consumer.currentAssignment()).hasSize(0);
    }

    @Test
    void testPosition() {
        ConsumerBuilder<String, String> consumer = companion.consumeStrings();

        try (ConsumerTask<String, String> records = consumer.fromTopics(topic)) {
            companion.produceStrings().withConcurrency().usingGenerator(i -> record(topic, "v" + i), 200);

            records.awaitRecords(100, Duration.ofMinutes(1));

            assertThat(consumer.position(tp(topic, 0))).isGreaterThanOrEqualTo(100L);
        }
        // position after closing consumer
        assertThatThrownBy(() -> consumer.position(tp(topic, 0)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCommitted() {
        ConsumerBuilder<String, String> consumer = companion.consumeStrings()
                .withCommitSyncWhen(cr -> true);

        try (ConsumerTask<String, String> records = consumer.fromTopics(topic)) {
            companion.produceStrings().withConcurrency().usingGenerator(i -> record(topic, "v" + i), 200);

            records.awaitRecords(100);
            await().untilAsserted(() -> assertThat(consumer.committed(tp(topic, 0)).offset())
                    .isGreaterThanOrEqualTo(100L));

            records.awaitRecords(200);
            await().untilAsserted(() -> assertThat(consumer.committed(tp(topic, 0)).offset()).isEqualTo(200L));
        }
    }

    @Test
    void testConsumerReuse() {
        ConsumerBuilder<String, String> consumer = companion.consumeStrings()
                .withCommitAsyncWhen(cr -> true);

        ConsumerTask<String, String> records = consumer.fromTopics(topic, 100);

        companion.produceStrings().withConcurrency().usingGenerator(i -> record(topic, "v-" + i), 400);

        assertThat(records.awaitCompletion().count()).isEqualTo(100);
        await().untilAsserted(() -> assertThat(consumer.committed(tp(topic, 0)).offset()).isEqualTo(100L));

        ConsumerTask<String, String> records2 = consumer.fromTopics(topic, 300);

        assertThat(records2.awaitCompletion().count()).isEqualTo(300);
        await().untilAsserted(() -> assertThat(consumer.committed(tp(topic, 0)).offset()).isEqualTo(400L));
    }

    @Test
    void testConsumerReuseFail() {
        ConsumerBuilder<String, String> consumer = companion.consumeStrings();
        try (ConsumerTask<String, String> records = consumer.fromTopics(topic)) {
            consumer.fromTopics(topic)
                    .awaitCompletion((failure, cancelled) -> assertThat(failure).isInstanceOf(IllegalStateException.class));
        }
    }

    @Test
    void testAwait() {
        long count = companion.produceDoubles()
                .usingGenerator(i -> record(topic, (double) i), Duration.ofSeconds(3))
                .awaitCompletion()
                .count();

        assertThat(companion.consumeDoubles()
                .fromTopics(topic, count)
                .awaitCompletion()
                .count()).isEqualTo(count);
    }

    @Test
    void testPauseResume() throws InterruptedException {
        companion.produceIntegers()
                .withConcurrency()
                .usingGenerator(i -> record(topic, i), 400);

        ConsumerBuilder<String, Integer> consumer = companion.consumeIntegers();

        try (ConsumerTask<String, Integer> records = consumer.fromTopics(topic)) {
            records.awaitRecords(100);

            consumer.pause();

            Thread.sleep(1000);
            // Consumption does not advance
            await().untilAsserted(() -> {
                long count = records.count();
                Thread.sleep(100);
                assertThat(records.count()).isEqualTo(count);
            });

            consumer.resume();
            records.awaitRecords(400, Duration.ofMinutes(1));
        }
    }

    @Test
    void testCommitAndClose() {
        // produce records
        companion.produceIntegers()
                .withConcurrency()
                .usingGenerator(i -> new ProducerRecord<>(topic, i), 100)
                .awaitCompletion();

        String groupId = UUID.randomUUID().toString();
        // consume topics
        companion.consumeStrings()
                .withAutoCommit()
                .withGroupId(groupId)
                .fromTopics(topic, 100)
                .awaitCompletion();

        await().untilAsserted(() -> assertThat(companion.consumerGroups().offsets(groupId, tp(topic, 0)))
                .isNotNull()
                .extracting(OffsetAndMetadata::offset).isEqualTo(100L));

        // commit offset to zero
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp(topic, 0), new OffsetAndMetadata(0));
        companion.consumeIntegers()
                .withGroupId(groupId)
                .commitAndClose(offsets);

        await().untilAsserted(() -> assertThat(companion.consumerGroups().offsets(groupId, tp(topic, 0)))
                .isNotNull()
                .extracting(OffsetAndMetadata::offset).isEqualTo(0L));

    }

    @Test
    void testAwaitNoRecords() {
        try (ConsumerTask<String, String> task = companion.consumeStrings().fromTopics(topic)) {
            assertThat(task.awaitNoRecords(Duration.ofSeconds(2)).count()).isZero();
            companion.produceStrings().fromRecords(new ProducerRecord<>(topic, "value"));
            assertThatThrownBy(() -> task.awaitNoRecords(Duration.ofSeconds(5)))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("expecting no records");
        }
    }

    @Test
    void testFromPrevious() {
        companion.topics().createAndWait(topic, 3);
        ProducerTask producerTask = companion.produceStrings().fromRecords(
                new ProducerRecord<>(topic, 0, "0", "1"),
                new ProducerRecord<>(topic, 1, "0", "2"),
                new ProducerRecord<>(topic, 1, "1", "3"),
                new ProducerRecord<>(topic, 2, "0", "4"),
                new ProducerRecord<>(topic, 0, "1", "5"),
                new ProducerRecord<>(topic, 0, "2", "6"))
                .awaitCompletion();

        assertThat(producerTask.latestOffsets())
                .containsEntry(tp(topic, 0), 2L)
                .containsEntry(tp(topic, 1), 1L)
                .containsEntry(tp(topic, 2), 0L);

        ConsumerTask<String, String> consumerTask = companion.consumeStrings().fromPrevious(producerTask);

        companion.produceStrings().fromRecords(
                new ProducerRecord<>(topic, 0, "3", "7"),
                new ProducerRecord<>(topic, 1, "2", "8"),
                new ProducerRecord<>(topic, 2, "1", "9")).awaitCompletion();

        consumerTask.awaitRecords(3);
        assertThat(consumerTask.latestOffsets())
                .containsEntry(tp(topic, 0), 3L)
                .containsEntry(tp(topic, 1), 2L)
                .containsEntry(tp(topic, 2), 1L);

        ConsumerTask<String, String> consumerTask2 = companion.consumeStrings().fromPrevious(consumerTask, 3);

        companion.produceStrings().fromRecords(
                new ProducerRecord<>(topic, 0, "4", "10"),
                new ProducerRecord<>(topic, 1, "3", "11"),
                new ProducerRecord<>(topic, 2, "21", "12")).awaitCompletion();

        assertThat(consumerTask2.awaitCompletion().latestOffsets()).hasSize(3);
    }
}
