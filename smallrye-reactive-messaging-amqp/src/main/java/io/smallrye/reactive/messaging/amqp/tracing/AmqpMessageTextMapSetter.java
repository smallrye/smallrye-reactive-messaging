package io.smallrye.reactive.messaging.amqp.tracing;

import io.opentelemetry.context.propagation.TextMapSetter;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.vertx.core.json.JsonObject;

public enum AmqpMessageTextMapSetter implements TextMapSetter<AmqpMessage<?>> {
    INSTANCE;

    @Override
    public void set(final AmqpMessage<?> carrier, final String key, final String value) {
        if (carrier != null) {
            JsonObject appProps = carrier.getApplicationProperties();
            JsonObject props = appProps == null ? JsonObject.of(key, value) : appProps.put(key, value);
            io.vertx.mutiny.amqp.AmqpMessage.create(carrier.getAmqpMessage()).applicationProperties(props).build();
        }
    }
}
