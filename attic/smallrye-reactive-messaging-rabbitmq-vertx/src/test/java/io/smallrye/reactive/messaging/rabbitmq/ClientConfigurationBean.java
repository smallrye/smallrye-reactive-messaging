package io.smallrye.reactive.messaging.rabbitmq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;
import io.vertx.rabbitmq.RabbitMQOptions;

@ApplicationScoped
public class ClientConfigurationBean {

    @Produces
    @Identifier("myclientoptions")
    public RabbitMQOptions options() {
        return new RabbitMQOptions()
                .setHost(System.getProperty("rabbitmq-host"))
                .setPort(Integer.parseInt(System.getProperty("rabbitmq-port")))
                .setUser(System.getProperty("rabbitmq-username"))
                .setPassword(System.getProperty("rabbitmq-password"));
    }

}
