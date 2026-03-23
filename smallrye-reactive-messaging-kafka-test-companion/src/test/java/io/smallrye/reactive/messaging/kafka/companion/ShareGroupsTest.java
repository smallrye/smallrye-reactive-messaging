package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.tp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.companion.test.KafkaCompanionTestBase;

public class ShareGroupsTest extends KafkaCompanionTestBase {

    @Test
    void testDescribeShareGroup() {
        companion.topics().createAndWait(topic, 1);

        String groupId = "share-test-" + UUID.randomUUID();
        companion.consumerGroups().alterShareGroupConfig(groupId, "share.auto.offset.reset", "earliest");
        try (ConsumerTask<String, Integer> task = companion.shareConsumeIntegers()
                .withGroupId(groupId)
                .withOffsetReset("earliest")
                .fromTopics(topic)) {

            companion.consumerGroups().waitForShareGroupAssignment(groupId)
                    .await().atMost(Duration.ofSeconds(20));

            companion.produceIntegers().fromRecords(
                    new ProducerRecord<>(topic, 1),
                    new ProducerRecord<>(topic, 2),
                    new ProducerRecord<>(topic, 3)).awaitCompletion();

            task.awaitRecords(3);

            await().untilAsserted(() -> {
                ShareGroupDescription description = companion.consumerGroups().describeShareGroup(groupId);
                assertThat(description).isNotNull();
                assertThat(description.groupId()).isEqualTo(groupId);
                assertThat(description.members()).isNotEmpty();
            });

            // Test varargs overload
            Map<String, ShareGroupDescription> descriptions = companion.consumerGroups().describeShareGroups(groupId);
            assertThat(descriptions).containsKey(groupId);
            assertThat(descriptions.get(groupId).members()).isNotEmpty();

        }
    }

    @Test
    void testShareGroupOffsets() {
        companion.topics().createAndWait(topic, 1);

        String groupId = "share-offsets-" + UUID.randomUUID();
        companion.consumerGroups().alterShareGroupConfig(groupId, "share.auto.offset.reset", "earliest");
        try (ConsumerTask<String, Integer> task = companion.shareConsumeIntegers()
                .withGroupId(groupId)
                .withOffsetReset("earliest")
                .fromTopics(topic)) {

            companion.consumerGroups().waitForShareGroupAssignment(groupId)
                    .await().atMost(Duration.ofSeconds(20));

            companion.produceIntegers().fromRecords(
                    new ProducerRecord<>(topic, 1),
                    new ProducerRecord<>(topic, 2),
                    new ProducerRecord<>(topic, 3),
                    new ProducerRecord<>(topic, 4),
                    new ProducerRecord<>(topic, 5)).awaitCompletion();

            task.awaitRecords(5, Duration.ofSeconds(20));

            // Test shareGroupOffsets (all partitions)
            await().untilAsserted(() -> {
                Map<TopicPartition, SharePartitionOffsetInfo> offsets = companion.consumerGroups()
                        .shareGroupOffsets(groupId);
                assertThat(offsets).isNotEmpty();
                assertThat(offsets).containsKey(tp(topic, 0));
            });

            // Test shareGroupOffsets (specific partition)
            TopicPartition tp = tp(topic, 0);
            Map<TopicPartition, SharePartitionOffsetInfo> partitionOffsets = companion.consumerGroups()
                    .shareGroupOffsets(groupId, List.of(tp));
            assertThat(partitionOffsets).containsKey(tp);
        }
    }

    @Test
    void testListGroupsIncludesShareGroups() {
        companion.topics().createAndWait(topic, 1);

        String groupId = "share-list-" + UUID.randomUUID();
        companion.consumerGroups().alterShareGroupConfig(groupId, "share.auto.offset.reset", "earliest");
        try (ConsumerTask<String, Integer> task = companion.shareConsumeIntegers()
                .withGroupId(groupId)
                .withOffsetReset("earliest")
                .fromTopics(topic)) {

            companion.consumerGroups().waitForShareGroupAssignment(groupId)
                    .await().atMost(Duration.ofSeconds(20));

            companion.produceIntegers().fromRecords(
                    new ProducerRecord<>(topic, 1)).awaitCompletion();

            task.awaitRecords(1);

            // listGroups should include share groups
            await().untilAsserted(() -> {
                var groups = companion.consumerGroups().listGroups();
                assertThat(groups).anyMatch(g -> g.groupId().equals(groupId));
            });

        }
    }

    @Test
    void testDescribeShareGroupMembers() {
        companion.topics().createAndWait(topic, 1);

        String groupId = "share-members-" + UUID.randomUUID();
        String clientId = "client-" + UUID.randomUUID();

        companion.consumerGroups().alterShareGroupConfig(groupId, "share.auto.offset.reset", "earliest");
        try (ConsumerTask<String, Integer> task = companion.shareConsumeIntegers()
                .withGroupId(groupId)
                .withClientId(clientId)
                .withOffsetReset("earliest")
                .fromTopics(topic)) {

            companion.consumerGroups().waitForShareGroupAssignment(groupId)
                    .await().atMost(Duration.ofSeconds(20));

            companion.produceIntegers().fromRecords(
                    new ProducerRecord<>(topic, 1)).awaitCompletion();

            task.awaitRecords(1);

            await().untilAsserted(() -> {
                ShareGroupDescription description = companion.consumerGroups().describeShareGroup(groupId);
                assertThat(description.members()).isNotEmpty();

                ShareMemberDescription member = description.members().stream()
                        .filter(m -> m.clientId().equals(clientId))
                        .findFirst().orElse(null);
                assertThat(member).isNotNull();
                assertThat(member.assignment().topicPartitions()).isNotEmpty();
            });

        }
    }

    @Test
    void testShareConsumerBuilder() {
        companion.topics().createAndWait(topic, 1);

        // Consume 5 records with explicit-acknowledge
        ShareConsumerBuilder<String, Integer> builder = companion.shareConsumeIntegers()
                .withOffsetReset("earliest")
                .withExplicitAck(r -> AcknowledgeType.ACCEPT);
        companion.consumerGroups().alterShareGroupConfig(builder.groupId(), "share.auto.offset.reset", "earliest");
        try (ConsumerTask<String, Integer> task = builder.fromTopics(topic, 5)) {

            companion.consumerGroups().waitForShareGroupAssignment(builder.groupId())
                    .await().atMost(Duration.ofSeconds(20));

            companion.produceIntegers().fromRecords(
                    new ProducerRecord<>(topic, 1),
                    new ProducerRecord<>(topic, 2),
                    new ProducerRecord<>(topic, 3),
                    new ProducerRecord<>(topic, 4),
                    new ProducerRecord<>(topic, 5)).awaitCompletion();

            task.awaitCompletion(Duration.ofSeconds(20));
            assertThat(task.count()).isEqualTo(5);
            assertThat(task.getRecords()).extracting(r -> r.value()).containsExactly(1, 2, 3, 4, 5);
        }
    }
}
