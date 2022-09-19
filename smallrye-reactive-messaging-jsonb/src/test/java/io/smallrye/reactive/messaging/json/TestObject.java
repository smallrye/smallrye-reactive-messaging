package io.smallrye.reactive.messaging.json;

import jakarta.json.bind.annotation.JsonbProperty;

/**
 * Test object to use in {@link JsonMapping} implementation.
 */
public class TestObject {
    /**
     * Weird casing to check proper (de)serialization.
     */
    @JsonbProperty("my_id")
    private int id;

    /**
     * Weird casing to check proper (de)serialization.
     */
    @JsonbProperty("my_Payload")
    private String payload;
}
