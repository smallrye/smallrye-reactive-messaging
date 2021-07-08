package io.smallrye.reactive.messaging.json;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Test object to use in {@link JsonMapping} implementation.
 */
public class TestObject {
    /**
     * Weird casing to check proper (de)serialization.
     */
    @JsonProperty("my_id")
    private int id;

    /**
     * Weird casing to check proper (de)serialization.
     */
    @JsonProperty("my_Payload")
    private String payload;
}
