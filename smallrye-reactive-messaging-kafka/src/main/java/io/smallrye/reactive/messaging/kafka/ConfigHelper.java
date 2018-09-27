package io.smallrye.reactive.messaging.kafka;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ConfigHelper {

  public static String getOrDie(Map<String, String> config, String key) {
    String value = get(config, key);
    if (value == null) {
      throw new IllegalStateException("Invalid configuration - expected key `" + key + "` to be present in " + config);
    }
    return value;
  }

  public static String get(Map<String, String> config, String key) {
    return Objects.requireNonNull(config).get(Objects.requireNonNull(key));
  }

  public static boolean getAsBoolean(Map<String, String> config, String key, boolean def) {
    String value = get(config, key);
    if (value == null) {
      return def;
    }
    return Boolean.valueOf(value);
  }

  public static int getAsInteger(Map<String, String> config, String key, int def) {
    String value = get(config, key);
    if (value == null) {
      return def;
    }
    try {
      return Integer.valueOf(value);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  public static Optional<Long> getAsLong(Map<String, String> config, String key) {
    String value = get(config, key);
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.valueOf(value));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

}
