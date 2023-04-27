package io.smallrye.reactive.messaging.kafka.companion;

import static io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion.toUni;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

/**
 * Companion for Offsets operations on Kafka broker
 * <p>
 * See {@link ConsumerGroupsCompanion} for consumer group offsets
 */
public class OffsetsCompanion {

    final AdminClient adminClient;
    final Duration kafkaApiTimeout;

    public OffsetsCompanion(AdminClient adminClient, Duration kafkaApiTimeout) {
        this.adminClient = adminClient;
        this.kafkaApiTimeout = kafkaApiTimeout;
    }

    /**
     * @param partitions the map of topic partition to {@link OffsetSpec}
     * @return the map of topic partition to Offset
     */
    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> list(
            Map<TopicPartition, OffsetSpec> partitions) {
        return toUni(() -> adminClient.listOffsets(partitions).all())
                .await().atMost(kafkaApiTimeout);
    }

    /**
     * @param partitions topic partitions
     * @return the latest offsets for given partitions
     */
    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> list(List<TopicPartition> partitions) {
        return list(partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())));
    }

    /**
     * @param topicPartition the topic partition
     * @param offsetSpec the offset spec
     * @return the offset result
     */
    public ListOffsetsResult.ListOffsetsResultInfo get(TopicPartition topicPartition, OffsetSpec offsetSpec) {
        Map<TopicPartition, OffsetSpec> offsets = new HashMap<>();
        offsets.put(topicPartition, offsetSpec);
        return toUni(() -> adminClient.listOffsets(offsets).partitionResult(topicPartition))
                .await().atMost(kafkaApiTimeout);
    }

}
