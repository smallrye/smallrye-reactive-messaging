package io.smallrye.reactive.messaging.pulsar.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum PulsarTraceTextMapSetter implements TextMapSetter<PulsarTrace> {
    INSTANCE;

    @Override
    public void set(final PulsarTrace carrier, final String key, final String value) {
        if (carrier != null) {
            Map<String, String> properties = carrier.getProperties();
            if (properties != null) {
                properties.put(key, value);
            }
        }
    }
}
