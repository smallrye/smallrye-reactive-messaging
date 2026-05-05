package io.smallrye.reactive.messaging.rabbitmq.og.reply;

public class RabbitMQRequestReplyTimeoutException extends RuntimeException {

    public RabbitMQRequestReplyTimeoutException(CorrelationId correlationId) {
        super("Timeout waiting for a reply for request with correlation ID: " + correlationId);
    }
}
