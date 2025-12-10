package io.smallrye.reactive.messaging.json;

import java.lang.reflect.Type;

/**
 * Interface to abstract json serialization to/from string.
 */
public interface JsonMapping {

    /**
     * Default priority of corresponding provider.
     *
     * @implNote could be used to control the load/init order in case multiple providers are specified/included.
     */
    int DEFAULT_PRIORITY = 500;

    /**
     * Serialize an object to JSON.
     *
     * @param object object to serialize
     * @return JSON representation of the object
     */
    String toJson(Object object);

    /**
     * Deserialize an object from it's JSON string representation.
     *
     * @param str JSON string
     * @param type type of object
     * @param <T> generic parametrization class
     * @return object of requested class
     */
    <T> T fromJson(String str, Class<T> type);

    /**
     * Deserialize an object from it's JSON string representation.
     *
     * @param str JSON string
     * @param type type of object
     * @param <T> generic parametrization class
     * @return object of requested class
     */
    default <T> T fromJson(String str, Type type) {
        throw new UnsupportedOperationException("Deserialization for a java.lang.reflect.Type is not supported");
    }
}
