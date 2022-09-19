package acme;

import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;
import io.vertx.amqp.AmqpClientOptions;

public class AmqpConfiguration {

    @Produces
    @Identifier("my-topic-config")
    public AmqpClientOptions options() {
        return new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUsername("smallrye")
                .setPassword("smallrye");
    }

    @Produces
    @Identifier("my-topic-config2")
    public AmqpClientOptions options2() {
        return new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUsername("smallrye")
                .setPassword("smallrye");
    }

}
