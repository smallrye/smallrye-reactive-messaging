package io.smallrye.reactive.messaging.amqp;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Named;

import io.vertx.amqp.AmqpClientOptions;

@ApplicationScoped
public class ClientConfigurationBean {

    @Produces
    @Named("myclientoptions")
    public AmqpClientOptions options() {
        return new AmqpClientOptions()
                .setHost(System.getProperty("amqp-host"))
                .setPort(Integer.valueOf(System.getProperty("amqp-port")))
                .setUsername(System.getProperty("amqp-user"))
                .setPassword(System.getProperty("amqp-pwd"));
    }

}
