package io.smallrye.reactive.messaging.jms.tracing;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum JmsTraceTextMapGetter implements TextMapGetter<JmsTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final JmsTrace carrier) {
        return carrier.getPropertyNames();
    }

    @Override
    public String get(final JmsTrace carrier, final String key) {
        if (carrier != null) {
            return carrier.getProperty(key);
        }
        return null;
    }
}
