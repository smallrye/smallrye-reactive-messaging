package io.smallrye.reactive.messaging.pulsar;

import java.util.Set;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

public interface PulsarClientService {

    /**
     * Gets the managed Pulsar Consumer for the given channel.
     *
     * @param channel the channel, must not be {@code null}
     * @param <T> the type of the value
     * @return the consumer, {@code null} if not found
     */
    <T> Consumer<T> getConsumer(String channel);

    /**
     * Gets the managed Pulsar Producer for the given channel.
     *
     * @param channel the channel, must not be {@code null}
     * @param <T> the type of the value
     * @return the consumer, {@code null} if not found
     */
    <T> Producer<T> getProducer(String channel);

    /**
     * Gets the managed Pulsar Client for the given channel.
     *
     * @param channel the channel, must not be {@code null}
     * @return the consumer, {@code null} if not found
     */
    PulsarClient getClient(String channel);

    /**
     * Get the names of all the Pulsar incoming channels managed by this connector.
     *
     * @return the names of the Pulsar consumer incoming channels.
     */
    Set<String> getConsumerChannels();

    /**
     * Get the names of all the Pulsar outgoing channels managed by this connector.
     *
     * @return the names of the Pulsar producer outgoing channels.
     */
    Set<String> getProducerChannels();
}
