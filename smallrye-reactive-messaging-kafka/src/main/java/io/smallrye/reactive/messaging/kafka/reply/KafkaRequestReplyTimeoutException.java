package io.smallrye.reactive.messaging.kafka.reply;

/**
 * Exception thrown when a reply is not received within the configured timeout.
 */
public class KafkaRequestReplyTimeoutException extends RuntimeException {

    public KafkaRequestReplyTimeoutException(CorrelationId correlationId) {
        super("Timeout waiting for a reply for request with correlation ID: " + correlationId);
    }

}
