package io.smallrye.reactive.messaging.kafka.tracing;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapPropagator;

public class HeaderExtractAdapter implements TextMapPropagator.Getter<Headers> {
    public static final HeaderExtractAdapter GETTER = new HeaderExtractAdapter();

    @Override
    public String get(Headers headers, String key) {
        final Header header = headers.lastHeader(key);
        if (header == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
