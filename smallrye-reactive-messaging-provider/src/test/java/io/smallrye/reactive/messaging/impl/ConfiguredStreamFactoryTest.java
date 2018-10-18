package io.smallrye.reactive.messaging.impl;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class ConfiguredStreamFactoryTest {


  @Test
  public void testSimpleExtraction() {
    Map<String, Object> backend = new HashMap<>();
    backend.put("foo", "bar");
    backend.put("io.prefix.name.k1", "v1");
    backend.put("io.prefix.name.k2", "v2");
    backend.put("io.prefix.name.k3.x", "v3");

    backend.put("io.prefix.name2.k1", "v1");
    backend.put("io.prefix.name2.k2", "v2");
    backend.put("io.prefix.name2.k3.x", "boo");
    backend.put("io.prefix.name2.another", "1");


    Config config = new DummyConfig(backend);
    Map<String, Map<String, String>> map = ConfiguredStreamFactory.extractConfigurationFor("io.prefix", config);

    assertThat(map).hasSize(2).containsKeys("name", "name2");
    assertThat(map.get("name")).hasSize(3).contains(entry("k1", "v1"), entry("k2", "v2"), entry("k3.x", "v3"));
    assertThat(map.get("name2")).hasSize(4).contains(entry("k1", "v1"), entry("k2", "v2"), entry("k3.x", "boo"), entry("another", "1"));
  }

  @Test
  public void testExtractionWithKeyAsName() {
    Map<String, Object> backend = new HashMap<>();
    backend.put("foo", "bar");
    backend.put("io.prefix.name", "the name");
    backend.put("io.prefix.name.k1", "v1");
    backend.put("io.prefix.name.k2", "v2");
    backend.put("io.prefix.name.k3.x", "v3");

    Config config = new DummyConfig(backend);
    Map<String, Map<String, String>> map = ConfiguredStreamFactory.extractConfigurationFor("io.prefix", config);

    assertThat(map).hasSize(1).containsKeys("name");
    assertThat(map.get("name")).hasSize(4).contains(entry("k1", "v1"), entry("k2", "v2"), entry("k3.x", "v3"), entry("name", "the name"));
  }


  private class DummyConfig implements Config {

    private final Map<String, Object> backend;

    private DummyConfig(Map<String, Object> backend) {
      this.backend = backend;
    }

    @Override
    public <T> T getValue(String s, Class<T> aClass) {
      return getOptionalValue(s, aClass).orElseThrow(() -> new IllegalArgumentException("Key not found: " + s));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> getOptionalValue(String s, Class<T> aClass) {
      return Optional.ofNullable((T) backend.get(s));
    }

    @Override
    public Iterable<String> getPropertyNames() {
      return backend.keySet();
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
      return Collections.emptyList();
    }
  }

}
