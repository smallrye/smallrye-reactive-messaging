package io.smallrye.reactive.messaging.providers.helpers;

/**
 * An exception that does not capture the stack trace.
 * Useful to nack messages without having to have to capture the stack trace.
 */
public class NoStackTraceException extends Exception {

    public NoStackTraceException(String msg) {
        super(msg);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
