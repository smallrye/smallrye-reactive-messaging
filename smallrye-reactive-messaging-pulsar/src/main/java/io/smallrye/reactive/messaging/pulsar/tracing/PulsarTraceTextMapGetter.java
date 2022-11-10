package io.smallrye.reactive.messaging.pulsar.tracing;

import java.util.Collections;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum PulsarTraceTextMapGetter implements TextMapGetter<PulsarTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final PulsarTrace carrier) {
        Map<String, String> properties = carrier.getProperties();
        if (properties != null) {
            return properties.keySet();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public String get(final PulsarTrace carrier, final String key) {
        if (carrier != null && carrier.getProperties() != null) {
            return carrier.getProperties().get(key);
        }
        return null;
    }
}
