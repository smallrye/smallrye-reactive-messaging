package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ConsumerGroupsTest extends KafkaCompanionTestBase {
    @Test
    void testListAndDeleteGroups() {
        companion.produceIntegers().fromRecords(
                new ProducerRecord<>(topic, 1),
                new ProducerRecord<>(topic, 2),
                new ProducerRecord<>(topic, 3),
                new ProducerRecord<>(topic, 4),
                new ProducerRecord<>(topic, 5)).awaitCompletion();

        String groupId = UUID.randomUUID().toString();
        ConsumerBuilder<String, Integer> consumer = companion.consumeIntegers().withGroupId(groupId);
        ConsumerTask<String, Integer> consumerTask = consumer.fromTopics(topic);

        // Wait until consumption starts
        consumerTask.awaitRecords(1);

        // Describe consumer group
        assertThat(companion.consumerGroups().list()).anyMatch(listing -> listing.groupId().equals(groupId));
        Map<String, ConsumerGroupDescription> descriptions = companion.consumerGroups().describe(new String[] { groupId });
        ConsumerGroupDescription groupDescription = descriptions.get(groupId);
        assertThat(groupDescription.isSimpleConsumerGroup()).isFalse();
        assertThat(groupDescription.state()).isEqualTo(ConsumerGroupState.STABLE);
        MemberDescription memberDescription = groupDescription.members().stream().findFirst().orElse(null);
        assertThat(memberDescription).isNotNull();
        String consumerId = memberDescription.consumerId();
        assertThat(consumerId).isNotNull();

        // Stop and wait until partitions revoked
        consumerTask.stop();
        await().until(() -> consumer.currentAssignment().isEmpty());

        // Delete consumer group
        companion.consumerGroups().delete(groupId);
    }

    @Test
    void testRemoveMember() {
        companion.produceIntegers().fromRecords(
                new ProducerRecord<>(topic, 1),
                new ProducerRecord<>(topic, 2),
                new ProducerRecord<>(topic, 3),
                new ProducerRecord<>(topic, 4),
                new ProducerRecord<>(topic, 5)).awaitCompletion();

        String groupId = UUID.randomUUID().toString();
        String groupInstanceId = UUID.randomUUID().toString();
        ConsumerTask<String, Integer> consumerTask = companion.consumeIntegers()
                .withGroupId(groupId)
                .withProp(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId)
                .fromTopics(topic);

        consumerTask.awaitRecords(1);

        companion.consumerGroups().removeMembers(groupId, groupInstanceId);
        assertThat(companion.consumerGroups().describe(groupId).members()).isEmpty();
    }

    @Test
    void testDeleteOffsets() {
        companion.produceIntegers().fromRecords(
                new ProducerRecord<>(topic, 1),
                new ProducerRecord<>(topic, 2),
                new ProducerRecord<>(topic, 3),
                new ProducerRecord<>(topic, 4),
                new ProducerRecord<>(topic, 5)).awaitCompletion();

        ConsumerBuilder<String, Integer> consumer = companion.consumeIntegers()
                .withClientId(UUID.randomUUID().toString())
                .withGroupId(UUID.randomUUID().toString())
                .withOffsetReset(OffsetResetStrategy.EARLIEST)
                .withCommitSyncWhen(r -> true);

        // Consume 5 recrds by committing offsets
        consumer.fromTopics(topic, 5).awaitCompletion();

        // Check committed offsets
        Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets = companion.consumerGroups()
                .offsets(Collections.singletonList(tp(topic, 0)));
        assertThat(offsets.get(consumer.groupId()).get(tp(topic, 0)).offset()).isEqualTo(5L);

        // Try to consume more with same groupId-consumerId
        ConsumerTask<String, Integer> task2 = consumer.fromTopics(topic);

        // Check that polling has started but no records consumed
        task2.awaitNoRecords(Duration.ofSeconds(5));

        // Stop and wait until partitions revoked
        task2.stop();
        await().until(consumer.currentAssignment()::isEmpty);

        // Delete offsets
        companion.consumerGroups().deleteOffsets(consumer.groupId(), Collections.singletonList(tp(topic, 0)));

        // Consume again, but this time check that 5 records are consumed
        ConsumerTask<String, Integer> consumer3 = companion.consumeIntegers().fromTopics(topic, 5);
        assertThat(consumer3.awaitCompletion().count()).isEqualTo(5L);
    }
}
