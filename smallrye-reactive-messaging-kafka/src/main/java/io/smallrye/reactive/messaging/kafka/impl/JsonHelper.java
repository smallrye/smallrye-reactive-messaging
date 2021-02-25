package io.smallrye.reactive.messaging.kafka.impl;

import java.util.Optional;

import org.eclipse.microprofile.config.Config;

import io.vertx.core.json.JsonObject;

/**
 * Be aware that this class is kafka specific.
 */
public class JsonHelper {

    public static JsonObject asJsonObject(Config config) {
        JsonObject json = new JsonObject();
        Iterable<String> propertyNames = config.getPropertyNames();
        for (String originalKey : propertyNames) {
            // Transform keys that may comes from environment variables.
            // As kafka properties use `.`, transform "_" into "."
            String key = originalKey.toLowerCase().replace("_", ".");
            try {
                Optional<Integer> i = config.getOptionalValue(key, Integer.class);
                if (i.isPresent() && i.get() instanceof Integer) {
                    json.put(key, i.get());
                    continue;
                }
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore me
            }

            try {
                Optional<Double> d = config.getOptionalValue(key, Double.class);
                if (d.isPresent() && d.get() instanceof Double) {
                    json.put(key, d.get());
                    continue;
                }
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore me
            }

            try {
                Optional<String> s = config.getOptionalValue(key, String.class);
                if (s.isPresent()) {
                    String value = s.get().trim();
                    if (value.equalsIgnoreCase("false")) {
                        json.put(key, false);
                    } else if (value.equalsIgnoreCase("true")) {
                        json.put(key, true);
                    } else {
                        json.put(key, value);
                    }
                    continue;
                }
            } catch (ClassCastException e) {
                // Ignore me
            }

            // We need to do boolean last, as it would return `false` for any non parsable object.
            try {
                Optional<Boolean> d = config.getOptionalValue(key, Boolean.class);
                d.ifPresent(v -> json.put(key, v));
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore the entry
            }

        }
        return json;
    }
}
