package io.smallrye.reactive.messaging.rabbitmq.tracing;

import java.util.Map;

public class RabbitMQTrace {
    private final String destination;
    private final Map<String, Object> headers;

    private RabbitMQTrace(final String destination, final Map<String, Object> headers) {
        this.destination = destination;
        this.headers = headers;
    }

    public String getDestination() {
        return destination;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public static RabbitMQTrace trace(
            final String destination,
            final Map<String, Object> headers) {
        return new RabbitMQTrace(destination, headers);
    }
}
