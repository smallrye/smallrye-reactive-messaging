package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.clients.producer.Producer;

import io.smallrye.common.annotation.Experimental;

@Experimental("experimental api")
public interface KafkaClientService {

    /**
     * Gets the managed Kafka Consumer for the given channel.
     * This method returns the reactive consumer.
     * <p>
     * Be aware that most actions requires to be run on the Kafka polling thread.
     * You can schedule actions using:
     * <p>
     * {@code getConsumer(channel).runOnPollingThread(c -> { ... })}
     * <p>
     * You can retrieve the <em>low-level</em> client using the {@link KafkaConsumer#unwrap()} method.
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
    <K, V> Producer<K, V> getProducer(String channel);

}
