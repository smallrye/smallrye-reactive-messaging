package io.smallrye.reactive.messaging.mqtt;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class MapBasedConfig implements Config {
  private final Map<String, Object> map;

  public MapBasedConfig(Map<String, Object> map) {
    this.map = map;
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
}
