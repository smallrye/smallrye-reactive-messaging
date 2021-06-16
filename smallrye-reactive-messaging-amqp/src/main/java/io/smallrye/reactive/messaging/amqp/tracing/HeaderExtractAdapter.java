package io.smallrye.reactive.messaging.amqp.tracing;

import java.nio.charset.StandardCharsets;

import io.opentelemetry.context.propagation.TextMapGetter;
import io.vertx.core.json.JsonObject;

public class HeaderExtractAdapter implements TextMapGetter<JsonObject> {
    public static final HeaderExtractAdapter GETTER = new HeaderExtractAdapter();

    private Iterable<String> keys;

    @Override
    public Iterable<String> keys(JsonObject properties) {
        if (keys == null) {
            keys = properties.fieldNames();
        }

        return keys;
    }

    @Override
    public String get(JsonObject properties, String key) {
        if (properties == null || properties.getBinary(key) == null) {
            return null;
        }
        return new String(properties.getBinary(key), StandardCharsets.UTF_8);
    }
}
