package io.smallrye.reactive.messaging.kafka.tracing;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapGetter;

public class HeaderExtractAdapter implements TextMapGetter<Headers> {
    public static final HeaderExtractAdapter GETTER = new HeaderExtractAdapter();

    private Iterable<String> keys;

    @Override
    public Iterable<String> keys(Headers headers) {
        if (keys == null) {
            keys = Arrays.stream(headers.toArray())
                    .map(Header::key)
                    .collect(Collectors.toList());
        }

        return keys;
    }

    @Override
    public String get(Headers headers, String key) {
        if (headers == null) {
            return null;
        }
        final Header header = headers.lastHeader(key);
        if (header == null) {
            return null;
        }
        byte[] headerValue = header.value();
        if (headerValue == null) {
            return null;
        }
        return new String(headerValue, StandardCharsets.UTF_8);
    }
}
