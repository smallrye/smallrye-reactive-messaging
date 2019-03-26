package io.smallrye.reactive.messaging.spi;

import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.Config;

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
    config.getPropertyNames().forEach(key -> json.put(key, config.getValue(key, Object.class)));
    return json;
  }
}
