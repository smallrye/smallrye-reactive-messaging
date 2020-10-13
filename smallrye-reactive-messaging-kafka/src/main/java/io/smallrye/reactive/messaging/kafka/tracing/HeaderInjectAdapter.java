package io.smallrye.reactive.messaging.kafka.tracing;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapPropagator;

class HeaderInjectAdapter implements TextMapPropagator.Setter<Headers> {
    public static final HeaderInjectAdapter SETTER = new HeaderInjectAdapter();

    @Override
    public void set(Headers headers, String key, String value) {
        if (headers != null) {
            headers.remove(key).add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
