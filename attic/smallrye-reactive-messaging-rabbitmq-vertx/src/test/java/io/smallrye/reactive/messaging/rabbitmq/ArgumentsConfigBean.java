package io.smallrye.reactive.messaging.rabbitmq;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
public class ArgumentsConfigBean {

    @Produces
    @Identifier("my-args")
    public Map<String, Object> produceArgs() {
        return Map.of("my-str-arg", "str-value", "my-int-arg", 4);
    }

    @Produces
    @Identifier("rabbitmq-queue-arguments")
    public Map<String, Object> defaultQueueArgs() {
        return Map.of("default-queue-arg", "default-value");
    }

    @Produces
    @Identifier("rabbitmq-exchange-arguments")
    public Map<String, Object> defaultExchangeArgs() {
        return Map.of("default-exchange-arg", "default-value");
    }
}
