package io.smallrye.reactive.messaging.http;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class HttpSinkConfig implements Config {
  private final Map<String, String> map;
  private final String prefix;

  public HttpSinkConfig(String name, String type, String url) {
    map = new HashMap<>();
    prefix = "smallrye.messaging." + type + "." + name + ".";
    map.put(prefix + "type", Http.class.getName());
    map.put(prefix + "url", url);
  }

  @Override
  public <T> T getValue(String propertyName, Class<T> propertyType) {
    return (T) map.get(propertyName);
  }

  @Override
  public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
    T value = getValue(propertyName, propertyType);
    return Optional.ofNullable(value);
  }

  @Override
  public Iterable<String> getPropertyNames() {
    return map.keySet();
  }

  @Override
  public Iterable<ConfigSource> getConfigSources() {
    return Collections.emptyList();
  }

  public Config converter(String className) {
    map.put(prefix + "converter", className);
    return this;
  }
}
