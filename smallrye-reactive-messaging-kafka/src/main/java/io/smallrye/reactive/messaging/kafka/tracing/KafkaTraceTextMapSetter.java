package io.smallrye.reactive.messaging.kafka.tracing;

import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum KafkaTraceTextMapSetter implements TextMapSetter<KafkaTrace> {
    INSTANCE;

    @Override
    public void set(final KafkaTrace carrier, final String key, final String value) {
        if (carrier != null) {
            Headers headers = carrier.getHeaders();
            headers.add(key, value.getBytes());
        }
    }
}
