package io.smallrye.reactive.messaging.rabbitmq.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RabbitMQTraceTextMapSetterTest {

    @Test
    void setAddsHeaderToCarrier() {
        Map<String, Object> headers = new HashMap<>();
        RabbitMQTrace trace = RabbitMQTrace.traceExchange("exchange", "rk", headers);

        RabbitMQTraceTextMapSetter.INSTANCE.set(trace, "traceparent", "00-abc-def-01");

        assertThat(headers).containsEntry("traceparent", "00-abc-def-01");
    }

    @Test
    void setOverwritesExistingHeader() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("traceparent", "old-value");
        RabbitMQTrace trace = RabbitMQTrace.traceExchange("exchange", "rk", headers);

        RabbitMQTraceTextMapSetter.INSTANCE.set(trace, "traceparent", "new-value");

        assertThat(headers).containsEntry("traceparent", "new-value");
    }

    @Test
    void setWithNullCarrierDoesNotThrow() {
        // Should silently do nothing
        RabbitMQTraceTextMapSetter.INSTANCE.set(null, "key", "value");
    }

    @Test
    void setWithNullHeadersDoesNotThrow() {
        RabbitMQTrace trace = RabbitMQTrace.traceExchange("exchange", "rk", null);

        // Should silently do nothing when headers are null
        RabbitMQTraceTextMapSetter.INSTANCE.set(trace, "key", "value");
    }

    @Test
    void setMultipleHeaders() {
        Map<String, Object> headers = new HashMap<>();
        RabbitMQTrace trace = RabbitMQTrace.traceExchange("exchange", "rk", headers);

        RabbitMQTraceTextMapSetter.INSTANCE.set(trace, "traceparent", "parent-val");
        RabbitMQTraceTextMapSetter.INSTANCE.set(trace, "tracestate", "state-val");

        assertThat(headers)
                .containsEntry("traceparent", "parent-val")
                .containsEntry("tracestate", "state-val")
                .hasSize(2);
    }
}
