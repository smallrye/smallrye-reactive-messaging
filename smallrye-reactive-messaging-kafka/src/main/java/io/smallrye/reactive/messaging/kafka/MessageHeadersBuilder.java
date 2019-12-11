package io.smallrye.reactive.messaging.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.header.internals.RecordHeader;

public class MessageHeadersBuilder {

    private final Map<String, byte[]> backend = new LinkedHashMap<>();

    public MessageHeadersBuilder with(MessageHeaders headers) {
        Objects.requireNonNull(headers, "headers");
        headers.unwrap().forEach(header -> {
            backend.put(header.key(), header.value());
        });
        return this;
    }

    public MessageHeadersBuilder with(String key, byte[] bytes) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(bytes, "bytes");
        backend.put(key, bytes);
        return this;
    }

    public MessageHeadersBuilder with(String key, ByteBuffer value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        backend.put(key, value.array());
        return this;
    }

    public MessageHeadersBuilder with(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        backend.put(key, value.getBytes());
        return this;
    }

    public MessageHeadersBuilder with(String key, String value, Charset encoding) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(encoding, "encoding");
        backend.put(key, value.getBytes(encoding));
        return this;
    }

    public MessageHeadersBuilder without(String key) {
        Objects.requireNonNull(key, "key");
        backend.remove(key);
        return this;
    }

    public MessageHeaders build() {
        return new MessageHeaders(backend.entrySet().stream()
                .map(entry -> new RecordHeader(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList()));
    }

}
