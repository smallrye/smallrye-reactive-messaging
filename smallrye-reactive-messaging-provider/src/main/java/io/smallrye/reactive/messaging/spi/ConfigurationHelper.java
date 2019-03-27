package io.smallrye.reactive.messaging.spi;

import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.Config;

import java.util.Iterator;
import java.util.Objects;

public class ConfigurationHelper {

  private final Config config;

  private ConfigurationHelper(Config conf) {
    this.config = conf;
  }

  public static ConfigurationHelper create(Config conf) {
    return new ConfigurationHelper(Objects.requireNonNull(conf));
  }

  public String getOrDie(String key) {
    return config.getOptionalValue(key, String.class).orElseThrow(() ->
      new IllegalArgumentException("Invalid configuration - expected key `" + key + "` to be present in " + config)
    );
  }

  public String get(String key) {
    return config.getOptionalValue(key, String.class).orElse(null);
  }

  public String get(String key, String def) {
    return config.getOptionalValue(key, String.class).orElse(def);
  }

  public boolean getAsBoolean(String key, boolean def) {
    return config.getOptionalValue(key, Boolean.class).orElse(def);
  }

  public int getAsInteger(String key, int def) {
    return config.getOptionalValue(key, Integer.class).orElse(def);
  }

  public JsonObject asJsonObject() {
    JsonObject json = new JsonObject();
    Iterable<String> propertyNames = config.getPropertyNames();
    Iterator<String> iterator = propertyNames.iterator();
    while (iterator.hasNext()) {
      String key = iterator.next();

      try {
        int i = config.getValue(key, Integer.class);
        json.put(key, i);
        continue;
      } catch (ClassCastException | NumberFormatException e) {
        // Ignore me
      }

      try {
        double d = config.getValue(key, Double.class);
        json.put(key, d);
        continue;
      } catch (ClassCastException | NumberFormatException e) {
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
