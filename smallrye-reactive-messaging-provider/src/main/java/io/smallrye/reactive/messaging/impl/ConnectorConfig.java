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
  private final boolean injectNameProperty;

  ConnectorConfig(String prefix, Config overall, String channel) {
    this.prefix = Objects.requireNonNull(prefix, "the prefix must not be set");
    this.overall = Objects.requireNonNull(overall, "the config must not be set");
    this.name = Objects.requireNonNull(channel, "the channel name must be set");
    this.injectNameProperty =
      StreamSupport.stream(overall.getPropertyNames().spliterator(), false)
        .noneMatch(s -> s.equalsIgnoreCase(prefix + ".name"));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getValue(String propertyName, Class<T> propertyType) {
    if (CHANNEL_NAME.equalsIgnoreCase(propertyName)  && injectNameProperty) {
      return (T) name;
    }
    return overall.getValue(prefix + "." + propertyName, propertyType);
  }

  @Override
  public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
    if ("channel-name".equalsIgnoreCase(propertyName)  && injectNameProperty) {
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
    if (injectNameProperty) {
      strings.add("channel-name");
    }
    return strings;
  }

  @Override
  public Iterable<ConfigSource> getConfigSources() {
    return overall.getConfigSources();
  }
}
