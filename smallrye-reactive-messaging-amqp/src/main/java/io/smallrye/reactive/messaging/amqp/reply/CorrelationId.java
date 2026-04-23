package io.smallrye.reactive.messaging.amqp.reply;

/**
 * Represents an identifier to correlate a request message and reply message.
 */
public abstract class CorrelationId {

    public abstract String toString();

    public abstract int hashCode();

    public abstract boolean equals(Object o);
}
