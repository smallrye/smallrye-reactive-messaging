package io.smallrye.reactive.messaging.amqp;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class MyConfig implements Config {
  private final Map<String, String> map;

  public MyConfig(Map<String, String> map) {
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
