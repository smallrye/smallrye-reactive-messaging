package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Objects;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.KafkaProducer;

@ApplicationScoped
public class KafkaClientServiceImpl implements KafkaClientService {

    @Inject
    @Connector(KafkaConnector.CONNECTOR_NAME)
    KafkaConnector connector;

    @Override
    public <K, V> KafkaConsumer<K, V> getConsumer(String channel) {
        return connector.getConsumer(Objects.requireNonNull(channel));
    }

    @Override
    public <K, V> KafkaProducer<K, V> getProducer(String channel) {
        return connector.getProducer(Objects.requireNonNull(channel));
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
