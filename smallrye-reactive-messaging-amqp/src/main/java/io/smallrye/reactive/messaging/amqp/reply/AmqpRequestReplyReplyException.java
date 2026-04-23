package io.smallrye.reactive.messaging.amqp.reply;

public class AmqpRequestReplyReplyException extends RuntimeException {

    private final boolean retryable;

    public AmqpRequestReplyReplyException(boolean retryable, String message) {
        super(message);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
