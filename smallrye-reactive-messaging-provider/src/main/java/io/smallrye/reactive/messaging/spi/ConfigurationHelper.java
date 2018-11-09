package io.smallrye.reactive.messaging.spi;

import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ConfigurationHelper {

  private final Map<String, String> config;

  private ConfigurationHelper(Map<String, String> conf) {
    this.config = conf;
  }

  public static ConfigurationHelper create(Map<String, String> conf) {
    return new ConfigurationHelper(Objects.requireNonNull(conf));
  }

  public String getOrDie(String key) {
    String value = get(key);
    if (value == null) {
      throw new IllegalArgumentException("Invalid configuration - expected key `" + key + "` to be present in " + config);
    }
    return value;
  }

  public String get(String key) {
    return Objects.requireNonNull(config).get(Objects.requireNonNull(key));
  }

  public boolean getAsBoolean(String key, boolean def) {
    String value = get(key);
    if (value == null) {
      return def;
    }
    return Boolean.valueOf(value);
  }

  public int getAsInteger(String key, int def) {
    String value = get(key);
    if (value == null) {
      return def;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  public Optional<Long> getAsLong(String key) {
    String value = get(key);
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.valueOf(value));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  public JsonObject asJsonObject() {
    JsonObject json = new JsonObject();
    config.forEach(json::put);
    return json;
  }
}
