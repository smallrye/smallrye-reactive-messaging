package io.smallrye.reactive.messaging.rabbitmq.og;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import com.rabbitmq.client.ConnectionFactory;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class ClientConfigurationBean {

    @Produces
    @Identifier("myclientoptions")
    public ConnectionFactory options() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(System.getProperty("rabbitmq-host"));
        factory.setPort(Integer.parseInt(System.getProperty("rabbitmq-port")));
        factory.setUsername(System.getProperty("rabbitmq-username"));
        factory.setPassword(System.getProperty("rabbitmq-password"));
        return factory;
    }

}
