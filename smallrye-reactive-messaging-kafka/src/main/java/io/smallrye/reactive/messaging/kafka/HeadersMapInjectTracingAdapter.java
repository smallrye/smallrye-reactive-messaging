package io.smallrye.reactive.messaging.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.kafka.common.header.Headers;

import io.opentracing.propagation.TextMap;

public class HeadersMapInjectTracingAdapter implements TextMap {

    private final Headers headers;

    HeadersMapInjectTracingAdapter(Headers headers) {
        this.headers = headers;
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
    }

    @Override
    public void put(String key, String value) {
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
}
