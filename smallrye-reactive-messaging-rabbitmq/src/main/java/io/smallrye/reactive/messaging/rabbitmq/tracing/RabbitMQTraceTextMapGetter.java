package io.smallrye.reactive.messaging.rabbitmq.tracing;

import java.util.Collections;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum RabbitMQTraceTextMapGetter implements TextMapGetter<RabbitMQTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final RabbitMQTrace carrier) {
        Map<String, Object> headers = carrier.getHeaders();
        if (headers != null) {
            return headers.keySet();
        }
        return Collections.emptyList();
    }

    @Override
    public String get(final RabbitMQTrace carrier, final String key) {
        if (carrier != null) {
            Map<String, Object> headers = carrier.getHeaders();
            if (headers != null) {
                Object value = headers.get(key);
                if (value != null) {
                    return value.toString();
                }
            }
        }
        return null;
    }
}
