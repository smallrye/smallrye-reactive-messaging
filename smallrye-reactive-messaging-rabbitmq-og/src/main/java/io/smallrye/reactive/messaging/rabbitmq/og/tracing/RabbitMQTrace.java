package io.smallrye.reactive.messaging.rabbitmq.og.tracing;

import java.util.Map;

/**
 * Represents tracing context for RabbitMQ messages.
 * Contains destination information and headers for OpenTelemetry context propagation.
 */
public class RabbitMQTrace {
    private final String destinationKind;
    private final String destination;
    private final String routingKey;
    private final Map<String, Object> headers;

    private RabbitMQTrace(final String destinationKind, final String destination, final String routingKey,
            final Map<String, Object> headers) {
        this.destination = destination;
        this.routingKey = routingKey;
        this.headers = headers;
        this.destinationKind = destinationKind;
    }

    public String getDestinationKind() {
        return destinationKind;
    }

    public String getDestination() {
        return destination;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public static RabbitMQTrace traceQueue(
            final String destination,
            final String routingKey,
            final Map<String, Object> headers) {
        return new RabbitMQTrace("queue", destination, routingKey, headers);
    }

    public static RabbitMQTrace traceExchange(
            final String destination,
            final String routingKey,
            final Map<String, Object> headers) {
        return new RabbitMQTrace("exchange", destination, routingKey, headers);
    }
}
