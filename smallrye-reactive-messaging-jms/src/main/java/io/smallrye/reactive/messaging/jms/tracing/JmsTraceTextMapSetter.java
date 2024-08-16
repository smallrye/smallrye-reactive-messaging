package io.smallrye.reactive.messaging.jms.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum JmsTraceTextMapSetter implements TextMapSetter<JmsTrace> {
    INSTANCE;

    @Override
    public void set(final JmsTrace carrier, final String key, final String value) {
        if (carrier != null) {
            Map<String, Object> messageProperties = carrier.getMessageProperties();
            messageProperties.put(key, value);
        }
    }
}
