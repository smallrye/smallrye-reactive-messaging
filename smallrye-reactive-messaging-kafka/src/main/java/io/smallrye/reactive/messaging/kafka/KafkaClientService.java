package io.smallrye.reactive.messaging.kafka;

import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import io.vertx.mutiny.kafka.client.producer.KafkaProducer;

public interface KafkaClientService {

    /**
     * Gets the managed Kafka Consumer for the given channel.
     *
     * @param channel the channel, must not be {@code null}
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the consumer, {@code null} if not found
     */
    <K, V> KafkaConsumer<K, V> getConsumer(String channel);

    /**
     * Gets the managed Kafka Producer for the given channel.
     *
     * @param channel the channel, must not be {@code null}
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the producer, {@code null} if not found
     */
    <K, V> KafkaProducer<K, V> getProducer(String channel);

}
