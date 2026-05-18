package io.smallrye.reactive.messaging.amqp.reply;

public class AmqpRequestReplyTimeoutException extends RuntimeException {
    public AmqpRequestReplyTimeoutException(String message) {
        super(message);
    }

    public AmqpRequestReplyTimeoutException(CorrelationId correlationId) {
        super("Timeout waiting for a reply for request with correlation ID: " + correlationId);
    }
}
