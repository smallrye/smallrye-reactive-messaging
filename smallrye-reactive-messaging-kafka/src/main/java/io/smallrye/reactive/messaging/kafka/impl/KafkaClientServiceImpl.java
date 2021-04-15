package io.smallrye.reactive.messaging.kafka.impl;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.kafka.KafkaClientService;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;

@ApplicationScoped
public class KafkaClientServiceImpl implements KafkaClientService {

    @Inject
    @Connector(KafkaConnector.CONNECTOR_NAME)
    KafkaConnector connector;

    @Override
    public <K, V> KafkaConsumer<K, V> getConsumer(String channel) {
        return connector.getConsumer(channel);
    }
}
