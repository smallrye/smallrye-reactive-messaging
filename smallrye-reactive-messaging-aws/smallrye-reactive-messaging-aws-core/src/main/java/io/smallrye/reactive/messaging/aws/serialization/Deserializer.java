package io.smallrye.reactive.messaging.aws.serialization;

/**
 * This interface describes how a {@link String} is deserialized to an object
 */
public interface Deserializer {

    /**
     * Deserialize a message to an object.
     *
     * @param payload message to deserialize
     * @return the deserialized message
     *         TODO: ex
     * @throws RuntimeException in case the deserialization fails
     */
    Object deserialize(String payload);
}
