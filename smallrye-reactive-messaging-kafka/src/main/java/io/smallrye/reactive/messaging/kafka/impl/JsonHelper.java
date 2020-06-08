package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Optional;

import org.eclipse.microprofile.config.Config;

import io.vertx.core.json.JsonObject;

public class JsonHelper {

    public static JsonObject asJsonObject(Config config) {
        JsonObject json = new JsonObject();
        Iterable<String> propertyNames = config.getPropertyNames();
        for (String key : propertyNames) {
            try {
                Optional<Integer> i = config.getOptionalValue(key, Integer.class);
                if (i.isPresent()) {
                    json.put(key, i.get());
                    continue;
                }
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore me
            }

            try {
                Optional<Double> d = config.getOptionalValue(key, Double.class);
                if (d.isPresent()) {
                    json.put(key, d.get());
                    continue;
                }
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore me
            }

            try {
                String value = config.getOptionalValue(key, String.class).orElse("").trim();
                if (value.equalsIgnoreCase("false")) {
                    json.put(key, false);
                } else if (value.equalsIgnoreCase("true")) {
                    json.put(key, true);
                } else {
                    json.put(key, value);
                }
            } catch (ClassCastException e) {
                // Ignore the entry
            }
        }
        return json;
    }
}
