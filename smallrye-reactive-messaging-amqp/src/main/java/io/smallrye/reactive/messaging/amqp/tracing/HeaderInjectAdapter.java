package io.smallrye.reactive.messaging.amqp.tracing;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import io.opentelemetry.context.propagation.TextMapSetter;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;

public class HeaderInjectAdapter implements TextMapSetter<AmqpMessage> {
    public static final HeaderInjectAdapter SETTER = new HeaderInjectAdapter();

    @Override
    public void set(AmqpMessage msg, String key, String value) {
        if (msg != null) {
            if (msg.applicationProperties() != null) {
                msg.applicationProperties().put(key, value.getBytes(StandardCharsets.UTF_8));
            } else {
                Properties props = new Properties();
                props.put(key, value.getBytes(StandardCharsets.UTF_8));
                msg = AmqpMessage.create(msg).applicationProperties(JsonObject.mapFrom(props)).build();
            }
        }
    }
}
