package io.smallrye.reactive.messaging.impl;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Implementation of config used to configured the different messaging provider / connector.
 */
public class ConnectorConfig implements Config {

  private static final String CHANNEL_NAME = "channel-name";
  private static final String CONNECTOR = "connector";

  private final String prefix;
  private final Config overall;

  private final String name;
  private final String connector;

  ConnectorConfig(String prefix, Config overall, String channel) {
    this.prefix = Objects.requireNonNull(prefix, "the prefix must not be set");
    this.overall = Objects.requireNonNull(overall, "the config must not be set");
    this.name = Objects.requireNonNull(channel, "the channel name must be set");

    Optional<String> value = overall.getOptionalValue(key(CONNECTOR), String.class);
    this.connector = value
      .orElseGet(() -> overall.getOptionalValue(key("type"), String.class) // Legacy
      .orElseThrow(() -> new IllegalArgumentException("Invalid channel configuration - " +
        "the `connector` attribute must be set for channel `" + name + "`")
      ));
    // Detect invalid channel-name attribute
    for (String key : overall.getPropertyNames()) {
      if ((key(CHANNEL_NAME)).equalsIgnoreCase(key)) {
        throw new IllegalArgumentException("Invalid channel configuration -  the `channel-name` attribute cannot be used" +
          " in configuration (channel `" + name + "`)");
      }
    }
  }

  private String key(String keyName) {
    return prefix + "." + name + "." + keyName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getValue(String propertyName, Class<T> propertyType) {
    if (CHANNEL_NAME.equalsIgnoreCase(propertyName)) {
      return (T) name;
    }
    if (CONNECTOR.equalsIgnoreCase(propertyName)  || "type".equalsIgnoreCase(propertyName)) {
      return (T) connector;
    }
    return overall.getValue(key(propertyName), propertyType);
  }

  @Override
  public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
    if (CHANNEL_NAME.equalsIgnoreCase(propertyName)) {
      return Optional.of((T) name);
    }
    if (CONNECTOR.equalsIgnoreCase(propertyName)  || "type".equalsIgnoreCase(propertyName)) {
      return Optional.of((T) connector);
    }
    return overall.getOptionalValue(key(propertyName), propertyType);
  }

  @Override
  public Iterable<String> getPropertyNames() {
    Set<String> strings = StreamSupport.stream(overall.getPropertyNames().spliterator(), false)
      .filter(s -> s.startsWith(prefix + "." + name + "."))
      .map(s -> s.substring((prefix + "." + name + ".").length()))
      .collect(Collectors.toSet());
    strings.add(CHANNEL_NAME);
    return strings;
  }

  @Override
  public Iterable<ConfigSource> getConfigSources() {
    return overall.getConfigSources();
  }
}
