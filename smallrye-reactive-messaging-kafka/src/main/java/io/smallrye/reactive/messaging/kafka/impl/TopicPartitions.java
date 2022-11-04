package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;

public class TopicPartitions {

    private static final Map<String, Map<Integer, TopicPartition>> TOPIC_PARTITIONS_CACHE = new ConcurrentHashMap<>();

    public static void clearCache() {
        TOPIC_PARTITIONS_CACHE.clear();
    }

    public static TopicPartition getTopicPartition(String topic, int partition) {
        return TOPIC_PARTITIONS_CACHE
                .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, p -> new TopicPartition(topic, partition));
    }

    public static TopicPartition getTopicPartition(KafkaRecord<?, ?> record) {
        return getTopicPartition(record.getTopic(), record.getPartition());
    }
}
