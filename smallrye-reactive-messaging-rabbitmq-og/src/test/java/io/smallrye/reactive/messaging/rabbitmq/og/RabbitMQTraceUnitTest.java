package io.smallrye.reactive.messaging.rabbitmq.og;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.rabbitmq.og.tracing.RabbitMQTrace;

/**
 * Tests for tracing support
 */
public class RabbitMQTraceUnitTest {

    @Test
    public void testTraceQueueCreation() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("test-header", "test-value");

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("my-queue", "my.routing.key", headers);

        assertThat(trace.getDestinationKind()).isEqualTo("queue");
        assertThat(trace.getDestination()).isEqualTo("my-queue");
        assertThat(trace.getRoutingKey()).isEqualTo("my.routing.key");
        assertThat(trace.getHeaders()).containsEntry("test-header", "test-value");
    }

    @Test
    public void testTraceExchangeCreation() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("correlation-id", "12345");

        RabbitMQTrace trace = RabbitMQTrace.traceExchange("my-exchange", "my.routing.key", headers);

        assertThat(trace.getDestinationKind()).isEqualTo("exchange");
        assertThat(trace.getDestination()).isEqualTo("my-exchange");
        assertThat(trace.getRoutingKey()).isEqualTo("my.routing.key");
        assertThat(trace.getHeaders()).containsEntry("correlation-id", "12345");
    }

    @Test
    public void testTraceWithEmptyHeaders() {
        Map<String, Object> headers = new HashMap<>();

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("queue", "routing.key", headers);

        assertThat(trace.getHeaders()).isEmpty();
    }

    @Test
    public void testTraceHeaderTypes() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("string-header", "value");
        headers.put("int-header", 42);
        headers.put("byte-array-header", "binary".getBytes(StandardCharsets.UTF_8));

        RabbitMQTrace trace = RabbitMQTrace.traceQueue("queue", "key", headers);

        assertThat(trace.getHeaders().get("string-header")).isEqualTo("value");
        assertThat(trace.getHeaders().get("int-header")).isEqualTo(42);
        assertThat(trace.getHeaders().get("byte-array-header")).isInstanceOf(byte[].class);
    }
}
