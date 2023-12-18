package io.smallrye.reactive.messaging.kafka.reply;

/**
 * Represents an identifier to correlate a request record and reply record.
 */
public abstract class CorrelationId {

    /**
     * Converts the object to a byte array representation.
     *
     * @return the byte array representation
     */
    public abstract byte[] toBytes();

    public abstract int hashCode();

    public abstract boolean equals(Object o);
}
