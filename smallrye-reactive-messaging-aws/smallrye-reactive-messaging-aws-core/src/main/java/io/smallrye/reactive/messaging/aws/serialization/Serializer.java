package io.smallrye.reactive.messaging.aws.serialization;

/**
 * This interface describes how an object is serialized to a {@link String}
 */
public interface Serializer {

    /**
     * Serialize an object to a {@link String}
     *
     * @param obj object to serialize
     * @return serialized message
     *         TODO: ex
     * @throws RuntimeException in case serialization fails.
     */
    String serialize(Object obj);
}
