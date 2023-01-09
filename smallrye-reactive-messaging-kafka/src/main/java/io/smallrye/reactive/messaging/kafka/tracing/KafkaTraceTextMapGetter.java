package io.smallrye.reactive.messaging.kafka.tracing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum KafkaTraceTextMapGetter implements TextMapGetter<KafkaTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final KafkaTrace carrier) {
        List<String> keys = new ArrayList<>();
        Headers headers = carrier.getHeaders();
        for (Header header : headers) {
            keys.add(header.key());
        }
        return keys;
    }

    @Override
    public String get(final KafkaTrace carrier, final String key) {
        if (carrier != null) {
            Header header = carrier.getHeaders().lastHeader(key);
            if (header != null) {
                byte[] value = header.value();
                if (value != null) {
                    return new String(value, StandardCharsets.UTF_8);
                }
            }
        }
        return null;
    }
}
