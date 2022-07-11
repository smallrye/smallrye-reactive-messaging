package io.smallrye.reactive.messaging.kafka;

import java.util.List;
import java.util.Set;

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
    default <K, V> KafkaConsumer<K, V> getConsumer(String channel) {
        List<KafkaConsumer<K, V>> consumers = getConsumers(channel);
        return consumers.stream().findFirst().orElse(null);
    }

    /**
     * Gets the list of managed Kafka Consumer for the given channel.
     * This method returns a reactive consumers, in the order of creation for multi-consumer channels.
     * <p>
     * 
     * @param channel the channel, must not be {@code null}
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the list of consumers, empty list if not found
     */
    <K, V> List<KafkaConsumer<K, V>> getConsumers(String channel);

    /**
     * Gets the managed Kafka Producer for the given channel.
     * This method returns the reactive producer.
     * <p>
     * Be aware that most actions require to be run on the Kafka sending thread.
     * You can schedule actions using:
     * <p>
     * {@code getProducer(channel).runOnSendingThread(c -> { ... })}
     * <p>
     * You can retrieve the <em>low-level</em> client using the {@link KafkaProducer#unwrap()} method.
     *
     * @param channel the channel, must not be {@code null}
     * @param <K> the type of the key
     * @param <V> the type of the value
     * @return the producer, {@code null} if not found
     */
    <K, V> KafkaProducer<K, V> getProducer(String channel);

    /**
     * Get the names of all the Kafka incoming channels managed by this connector.
     *
     * @return the names of the Kafka consumer incoming channels.
     */
    Set<String> getConsumerChannels();

    /**
     * Get the names of all the Kafka outgoing channels managed by this connector.
     *
     * @return the names of the Kafka producer outgoing channels.
     */
    Set<String> getProducerChannels();

}
