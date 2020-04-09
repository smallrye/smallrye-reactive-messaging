package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.*;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarManager.class);
    private final Config config;


    public PulsarManager(Config config){
        this.config = config;
    }


    public PulsarClient createNewClient() {
        StringBuilder url = new StringBuilder();
        String host = config.getValue("host", String.class);
        String port = config.getValue("port", String.class);
        url.append("pulsar://").append(host).append(":").append(port);
        try {
            return PulsarClient.builder()
                .serviceUrl(url.toString())
                .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }


    public Consumer createNewConsumer() {
        try {
            return createNewClient()
                .newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Key_Shared)
                .topic(config.getValue("topic",String.class))
                .subscriptionName("temp").subscribe();
        } catch (PulsarClientException e) {
            LOGGER.error(e.getMessage());
            return null;
        }
    }
}
