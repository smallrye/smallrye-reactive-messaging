package io.smallrye.reactive.messaging.pulsar;

import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

@ApplicationScoped
public class PulsarClientServiceImpl implements PulsarClientService {

    @Inject
    @Connector(PulsarConnector.CONNECTOR_NAME)
    PulsarConnector connector;

    @Override
    public <T> Consumer<T> getConsumer(String channel) {
        return connector.getConsumer(channel);
    }

    @Override
    public <T> Producer<T> getProducer(String channel) {
        return connector.getProducer(channel);
    }

    @Override
    public PulsarClient getClient(String channel) {
        return connector.getClient(channel);
    }

    @Override
    public Set<String> getConsumerChannels() {
        return connector.getConsumerChannels();
    }

    @Override
    public Set<String> getProducerChannels() {
        return connector.getProducerChannels();
    }
}
