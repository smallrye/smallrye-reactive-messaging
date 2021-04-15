package io.smallrye.reactive.messaging.kafka;

import io.smallrye.common.annotation.Experimental;

@Experimental("experimental api")
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

}
