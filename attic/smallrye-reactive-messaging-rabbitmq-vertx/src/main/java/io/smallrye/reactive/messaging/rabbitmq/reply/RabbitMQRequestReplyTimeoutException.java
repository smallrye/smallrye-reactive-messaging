package io.smallrye.reactive.messaging.rabbitmq.reply;

/**
 * Exception thrown when a reply is not received within the configured timeout.
 */
public class RabbitMQRequestReplyTimeoutException extends RuntimeException {

    public RabbitMQRequestReplyTimeoutException(CorrelationId correlationId) {
        super("Timeout waiting for a reply for request with correlation ID: " + correlationId);
    }
}
