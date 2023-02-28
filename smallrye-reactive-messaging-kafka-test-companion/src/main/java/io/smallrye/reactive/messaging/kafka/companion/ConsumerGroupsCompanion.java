package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.toUni;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.MemberToRemove;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;

/**
 * Companion for Consumer Group operations on Kafka broker
 */
public class ConsumerGroupsCompanion {

    final AdminClient adminClient;
    final Duration kafkaApiTimeout;

    public ConsumerGroupsCompanion(AdminClient adminClient, Duration kafkaApiTimeout) {
        this.adminClient = adminClient;
        this.kafkaApiTimeout = kafkaApiTimeout;
    }

    /**
     * @return the list of consumer groups
     */
    public Collection<ConsumerGroupListing> list() {
        return toUni(adminClient.listConsumerGroups().all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group ids
     * @return the map of consumer group descriptions by id
     */
    public Map<String, ConsumerGroupDescription> describe(String... groupId) {
        return toUni(adminClient.describeConsumerGroups(Arrays.asList(groupId)).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group id
     * @return the consumer group description
     */
    public ConsumerGroupDescription describe(String groupId) {
        return toUni(adminClient.describeConsumerGroups(Collections.singleton(groupId)).all())
                .onItem().transform(result -> result.get(groupId))
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group ids
     */
    public void delete(String... groupId) {
        toUni(adminClient.deleteConsumerGroups(Arrays.asList(groupId)).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId the consumer group id
     * @param groupInstanceIds group instance ids
     */
    public void removeMembers(String groupId, String... groupInstanceIds) {
        toUni(adminClient.removeMembersFromConsumerGroup(groupId,
                new RemoveMembersFromConsumerGroupOptions(Arrays.stream(groupInstanceIds)
                        .map(MemberToRemove::new).collect(Collectors.toList())))
                .all())
                .await().atMost(kafkaApiTimeout);
    }

    /*
     * OFFSETS
     */

    private Uni<Map<TopicPartition, OffsetAndMetadata>> consumerGroupUni(String groupId,
            List<TopicPartition> topicPartitions) {
        return toUni(adminClient.listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions()
                .topicPartitions(topicPartitions)).partitionsToOffsetAndMetadata());
    }

    /**
     * @param groupId consumer group id
     * @return the map of topic partitions to offset
     */
    public Map<TopicPartition, OffsetAndMetadata> offsets(String groupId) {
        return toUni(adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group id
     * @param topicPartitions list of topic partitions
     * @return the map of topic partitions to offset
     */
    public Map<TopicPartition, OffsetAndMetadata> offsets(String groupId, List<TopicPartition> topicPartitions) {
        return consumerGroupUni(groupId, topicPartitions).await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group id
     * @param topicPartitions topic partition
     * @return the offset
     */
    public OffsetAndMetadata offsets(String groupId, TopicPartition topicPartitions) {
        return consumerGroupUni(groupId, Collections.singletonList(topicPartitions))
                .onItem().transform(m -> m.get(topicPartitions))
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param topicPartitions list of topic partitions
     * @return map of consumer group id to topic partitions offset
     */
    public Map<String, Map<TopicPartition, OffsetAndMetadata>> offsets(List<TopicPartition> topicPartitions) {
        return toUni(adminClient.listConsumerGroups().all())
                .onItem().transformToMulti(groups -> Multi.createFrom().iterable(groups))
                .onItem()
                .transformToUniAndMerge(group -> consumerGroupUni(group.groupId(), topicPartitions)
                        .map(m -> Tuple2.of(group.groupId(), m)))
                .collect().asMap(Tuple2::getItem1, Tuple2::getItem2)
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group id
     * @param topicPartitionOffsets the map of topic partitions to offset
     */
    public void alterOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsets) {
        toUni(adminClient.alterConsumerGroupOffsets(groupId, topicPartitionOffsets).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param groupId consumer group id
     * @param partition topic partition
     */
    public void resetOffsets(String groupId, TopicPartition partition) {
        Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(partition, new OffsetAndMetadata(0L));
        alterOffsets(groupId, offsetMap);
    }

    /**
     * @param groupId consumer group id
     * @param topicPartitions list of topic partitions
     */
    public void deleteOffsets(String groupId, List<TopicPartition> topicPartitions) {
        toUni(adminClient.deleteConsumerGroupOffsets(groupId, new HashSet<>(topicPartitions)).all())
                .await().atMost(kafkaApiTimeout);
    }
}
