package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;

public record TopicPartitionKey(TopicPartition topicPartition, Object key) {

    private static final Object NO_KEY = new Object();

    public static TopicPartitionKey ofKey(String topic, int partition, Object key) {
        return new TopicPartitionKey(TopicPartitions.getTopicPartition(topic, partition), key);
    }

    public static TopicPartitionKey ofKey(ConsumerRecord<?, ?> record) {
        return new TopicPartitionKey(TopicPartitions.getTopicPartition(record.topic(), record.partition()), record.key());
    }

    public static TopicPartitionKey ofPartition(String topic, int partition) {
        return new TopicPartitionKey(TopicPartitions.getTopicPartition(topic, partition), NO_KEY);
    }

    public static TopicPartitionKey ofPartition(ConsumerRecord<?, ?> record) {
        return new TopicPartitionKey(TopicPartitions.getTopicPartition(record.topic(), record.partition()), NO_KEY);
    }

}
