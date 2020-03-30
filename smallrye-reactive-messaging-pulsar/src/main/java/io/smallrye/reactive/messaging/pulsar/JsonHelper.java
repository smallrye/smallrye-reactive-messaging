package io.smallrye.reactive.messaging.pulsar;

import org.eclipse.microprofile.config.Config;

import io.vertx.core.json.JsonObject;

public class JsonHelper {

    public static JsonObject asJsonObject(Config config) {
        JsonObject json = new JsonObject();
        Iterable<String> propertyNames = config.getPropertyNames();
        for (String key : propertyNames) {
            try {
                int i = config.getValue(key, Integer.class);
                json.put(key, i);
                continue;
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore me
            }

            try {
                double d = config.getValue(key, Double.class);
                json.put(key, d);
                continue;
            } catch (ClassCastException | IllegalArgumentException e) {
                // Ignore me
            }

            try {
                String value = config.getValue(key, String.class);
                if (value.trim().equalsIgnoreCase("false")) {
                    json.put(key, false);
                } else if (value.trim().equalsIgnoreCase("true")) {
                    json.put(key, true);
                } else {
                    json.put(key, config.getValue(key, String.class));
                }
            } catch (ClassCastException e) {
                // Ignore the entry
            }
        }
        return json;
    }
}
