package io.smallrye.reactive.messaging.amqp.tracing;

import java.util.Collections;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.vertx.core.json.JsonObject;

public enum AmqpMessageTextMapGetter implements TextMapGetter<AmqpMessage<?>> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final AmqpMessage<?> carrier) {
        JsonObject applicationProperties = carrier.getApplicationProperties();
        return applicationProperties != null ? applicationProperties.fieldNames() : Collections.emptyList();
    }

    public String get(final AmqpMessage<?> carrier, final String key) {
        JsonObject applicationProperties = carrier.getApplicationProperties();
        return applicationProperties != null ? applicationProperties.getString(key) : null;
    }
}
