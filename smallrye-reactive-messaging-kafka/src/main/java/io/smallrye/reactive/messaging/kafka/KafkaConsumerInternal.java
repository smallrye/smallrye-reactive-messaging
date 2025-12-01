package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import io.smallrye.mutiny.Uni;

public interface KafkaConsumerInternal<K, V> {

    Uni<ConsumerRecords<K, V>> poll();

    String get(String configKey);

    void close();
}
