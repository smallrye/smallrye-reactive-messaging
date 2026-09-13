package io.smallrye.reactive.messaging.rabbitmq.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RabbitMQTraceTextMapGetterTest {

    @Test
    void keysWithNonNullHeaders() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("traceparent", "00-abc-def-01");
        headers.put("tracestate", "key=value");

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", headers);

        Iterable<String> keys = RabbitMQTraceTextMapGetter.INSTANCE.keys(trace);
        assertThat(keys).containsExactlyInAnyOrder("traceparent", "tracestate");
    }

    @Test
    void keysWithNullHeaders() {
        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", null);

        Iterable<String> keys = RabbitMQTraceTextMapGetter.INSTANCE.keys(trace);
        assertThat(keys).isEmpty();
    }

    @Test
    void getWithExistingKey() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("traceparent", "00-abc-def-01");

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", headers);

        String value = RabbitMQTraceTextMapGetter.INSTANCE.get(trace, "traceparent");
        assertThat(value).isEqualTo("00-abc-def-01");
    }

    @Test
    void getWithMissingKey() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("traceparent", "00-abc-def-01");

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", headers);

        String value = RabbitMQTraceTextMapGetter.INSTANCE.get(trace, "nonexistent");
        assertThat(value).isNull();
    }

    @Test
    void getWithNullHeaders() {
        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", null);

        String value = RabbitMQTraceTextMapGetter.INSTANCE.get(trace, "traceparent");
        assertThat(value).isNull();
    }

    @Test
    void getWithNullCarrier() {
        String value = RabbitMQTraceTextMapGetter.INSTANCE.get(null, "traceparent");
        assertThat(value).isNull();
    }

    @Test
    void getWithNonStringHeaderValue() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-count", 42);

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", headers);

        String value = RabbitMQTraceTextMapGetter.INSTANCE.get(trace, "x-count");
        assertThat(value).isEqualTo("42");
    }

    @Test
    void getWithNullHeaderValue() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-null", null);

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("dest", "rk", headers);

        String value = RabbitMQTraceTextMapGetter.INSTANCE.get(trace, "x-null");
        assertThat(value).isNull();
    }
}
