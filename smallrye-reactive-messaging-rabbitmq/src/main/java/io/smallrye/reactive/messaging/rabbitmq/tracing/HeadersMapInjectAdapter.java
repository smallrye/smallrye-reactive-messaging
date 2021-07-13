package io.smallrye.reactive.messaging.rabbitmq.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * A {@link TextMapSetter} that targets a string-keyed Map.
 */
class HeadersMapInjectAdapter implements TextMapSetter<Map<String, Object>> {

    public static final HeadersMapInjectAdapter SETTER = new HeadersMapInjectAdapter();

    /**
     * Private constructor to prevent instantiation.
     */
    private HeadersMapInjectAdapter() {
    }

    @Override
    public void set(final Map<String, Object> carrier, final String key, final String value) {
        carrier.put(key, value);
    }
}
