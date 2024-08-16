package io.smallrye.reactive.messaging.jms.tracing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum JmsTraceTextMapGetter implements TextMapGetter<JmsTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final JmsTrace carrier) {
        List<String> keys = new ArrayList<>();
        Map<String, Object> messageProperties = carrier.getMessageProperties();
        for (String key : messageProperties.keySet()) {
            keys.add(key);
        }
        return keys;
    }

    @Override
    public String get(final JmsTrace carrier, final String key) {
        if (carrier != null) {
            Object value = carrier.getMessageProperties().get(key);
            if (value != null) {
                return value.toString();
            }
        }
        return null;
    }
}
