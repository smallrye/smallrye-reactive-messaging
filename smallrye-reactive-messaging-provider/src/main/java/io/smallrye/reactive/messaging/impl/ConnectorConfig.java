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

  private final String prefix;
  private final Config overall;

  public static final Config EMPTY_CONFIG = new Config() {

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
      return null;
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
      return Optional.empty();
    }

    @Override
    public Iterable<String> getPropertyNames() {
      return Collections.emptyList();
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
      return Collections.emptyList();
    }
  };
  private final String name;
  private final boolean injectNameProperty;

  public ConnectorConfig(String prefix, Config overall, String name) {
    this.prefix = Objects.requireNonNull(prefix, "the prefix must not be set");
    this.overall = Objects.requireNonNull(overall, "the config must not be set");
    this.name = Objects.requireNonNull(name, "the name must be set");
    this.injectNameProperty =
      StreamSupport.stream(overall.getPropertyNames().spliterator(), false)
        .noneMatch(s -> s.equalsIgnoreCase(prefix + ".name"));
    System.out.println("inject name property: " + injectNameProperty + " / " + overall.getPropertyNames() + " prefix: " + prefix);
  }

  @Override
  public <T> T getValue(String propertyName, Class<T> propertyType) {
    if ("name".equalsIgnoreCase(propertyName)  && injectNameProperty) {
      return (T) name;
    }
    return overall.getValue(prefix + "." + propertyName, propertyType);
  }

  @Override
  public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
    if ("name".equalsIgnoreCase(propertyName)  && injectNameProperty) {
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
    System.out.println(strings);
    if (injectNameProperty) {
      strings.add("name");
    }
    return strings;
  }

  @Override
  public Iterable<ConfigSource> getConfigSources() {
    return overall.getConfigSources();
  }
}
