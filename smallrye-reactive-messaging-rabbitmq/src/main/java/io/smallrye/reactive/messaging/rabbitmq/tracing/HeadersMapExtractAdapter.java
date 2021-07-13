package io.smallrye.reactive.messaging.rabbitmq.tracing;

import java.util.ArrayList;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

/**
 * A {@link TextMapGetter} that is sourced from a string-keyed Map.
 */
public class HeadersMapExtractAdapter implements TextMapGetter<Map<String, Object>> {

    public static final HeadersMapExtractAdapter GETTER = new HeadersMapExtractAdapter();

    /**
     * Constructor.
     */
    private HeadersMapExtractAdapter() {
    }

    @Override
    public Iterable<String> keys(final Map<String, Object> carrier) {
        return new ArrayList<>(carrier.keySet());
    }

    @Override
    public String get(final Map<String, Object> carrier, final String key) {
        return (carrier != null) ? String.valueOf(carrier.get(key)) : null;
    }
}
