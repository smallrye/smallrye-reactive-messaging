package acme;

import javax.enterprise.inject.Produces;
import javax.inject.Named;

import io.vertx.amqp.AmqpClientOptions;

public class AmqpConfiguration {

    @Produces
    @Named("my-topic-config")
    public AmqpClientOptions options() {
        return new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUsername("smallrye")
                .setPassword("smallrye");
    }

    @Produces
    @Named("my-topic-config2")
    public AmqpClientOptions options2() {
        return new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUsername("smallrye")
                .setPassword("smallrye");
    }

}
