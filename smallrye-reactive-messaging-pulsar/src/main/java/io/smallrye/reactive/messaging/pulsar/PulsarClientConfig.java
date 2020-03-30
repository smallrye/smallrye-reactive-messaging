package io.smallrye.reactive.messaging.pulsar;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

@ApplicationScoped
public class PulsarClientConfig {

    @Inject
    @ConfigProperty(name = "host")
    private String host;

    @Inject
    @ConfigProperty(name = "port")
    private String port;


    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }


    @Produces
    PulsarClient getClient(){
        StringBuilder url = new StringBuilder();
        url.append("pulsar://").append(host).append(":").append(url);
        try {
            PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(url.toString())
                .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }
}
