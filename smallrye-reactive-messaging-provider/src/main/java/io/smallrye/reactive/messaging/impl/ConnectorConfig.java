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
  private final String prefix;
  private final Config overall;

  private final String name;

  ConnectorConfig(String prefix, Config overall, String channel) {
    this.prefix = Objects.requireNonNull(prefix, "the prefix must not be set");
    this.overall = Objects.requireNonNull(overall, "the config must not be set");
    this.name = Objects.requireNonNull(channel, "the channel name must be set");

    // Detect invalid channel-name attribute
    for (String key : overall.getPropertyNames()) {
      if ((prefix + "." + CHANNEL_NAME).equalsIgnoreCase(key)) {
        throw new IllegalArgumentException("The `channel-name` attribute cannot be used in configuration, " +
          "it's automatically injected");
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getValue(String propertyName, Class<T> propertyType) {
    if (CHANNEL_NAME.equalsIgnoreCase(propertyName)) {
      return (T) name;
    }
    return overall.getValue(prefix + "." + propertyName, propertyType);
  }

  @Override
  public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
    if (CHANNEL_NAME.equalsIgnoreCase(propertyName)) {
      return Optional.of((T) name);
    }
    return overall.getOptionalValue(prefix + "." + propertyName, propertyType);
  }

  @Override
  public Iterable<String> getPropertyNames() {
    Set<String> strings = StreamSupport.stream(overall.getPropertyNames().spliterator(), false)
      .filter(s -> s.startsWith(prefix + "."))
      .map(s -> s.substring((prefix + ".").length()))
      .collect(Collectors.toSet());
    strings.add(CHANNEL_NAME);
    return strings;
  }

  @Override
  public Iterable<ConfigSource> getConfigSources() {
    return overall.getConfigSources();
  }
}
