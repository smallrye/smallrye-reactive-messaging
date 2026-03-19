package io.smallrye.reactive.messaging.rabbitmq.og.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * OpenTelemetry text map setter for injecting tracing context into RabbitMQ message headers.
 */
public enum RabbitMQTraceTextMapSetter implements TextMapSetter<RabbitMQTrace> {
    INSTANCE;

    @Override
    public void set(final RabbitMQTrace carrier, final String key, final String value) {
        if (carrier != null) {
            Map<String, Object> headers = carrier.getHeaders();
            if (headers != null) {
                headers.put(key, value);
            }
        }
    }
}
