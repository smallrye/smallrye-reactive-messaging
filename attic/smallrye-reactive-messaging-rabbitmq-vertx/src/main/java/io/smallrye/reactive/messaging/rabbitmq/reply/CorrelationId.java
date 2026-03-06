package io.smallrye.reactive.messaging.rabbitmq.reply;

/**
 * Represents an identifier to correlate a request message and reply message.
 */
public abstract class CorrelationId {

    /**
     * Converts the object to a string representation.
     *
     * @return the string representation
     */
    public abstract String toString();

    public abstract int hashCode();

    public abstract boolean equals(Object o);
}
