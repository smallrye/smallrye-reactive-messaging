package io.smallrye.reactive.messaging.amqp.tracing;

import static java.util.Collections.singletonMap;

import io.opentelemetry.context.propagation.TextMapSetter;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.vertx.core.json.JsonObject;

public enum AmqpMessageTextMapSetter implements TextMapSetter<AmqpMessage<?>> {
    INSTANCE;

    @Override
    public void set(final AmqpMessage<?> carrier, final String key, final String value) {
        if (carrier != null) {
            JsonObject applicationProperties = carrier.getApplicationProperties();
            if (applicationProperties != null) {
                applicationProperties.put(key, value.getBytes());
            } else {
                io.vertx.mutiny.amqp.AmqpMessage.create(carrier.getAmqpMessage()).applicationProperties(new JsonObject(
                        singletonMap(key, value))).build();
            }
        }
    }
}
