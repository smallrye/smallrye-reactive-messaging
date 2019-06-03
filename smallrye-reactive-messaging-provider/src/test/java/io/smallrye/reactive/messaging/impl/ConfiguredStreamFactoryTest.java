package io.smallrye.reactive.messaging.impl;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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
    Map<String, ConnectorConfig> map = ConfiguredChannelFactory.extractConfigurationFor("io.prefix", config);

    assertThat(map).hasSize(2).containsKeys("name", "name2");
    ConnectorConfig config1 = map.get("name");
    ConnectorConfig config2 = map.get("name2");
    assertThat(config1.getPropertyNames()).hasSize(4).contains("k1", "k2", "k3.x", "channel-name");
    assertThat(config1.getValue("k1", String.class)).isEqualTo("v1");
    assertThat(config1.getValue("k2", String.class)).isEqualTo("v2");
    assertThat(config1.getValue("k3.x", String.class)).isEqualTo("v3");
    assertThat(config1.getValue("channel-name", String.class)).isEqualTo("name");
    assertThat(config2.getPropertyNames()).hasSize(5).contains("k1", "k2", "k3.x", "another", "channel-name");
    assertThat(config2.getValue("k1", String.class)).isEqualTo("v1");
    assertThat(config2.getValue("k2", String.class)).isEqualTo("v2");
    assertThat(config2.getValue("k3.x", String.class)).isEqualTo("boo");
    assertThat(config2.getValue("channel-name", String.class)).isEqualTo("name2");
    assertThat(config2.getOptionalValue("another", String.class)).contains("1");
    assertThat(config2.getOptionalValue("missing", String.class)).isEmpty();
  }

  @Test
  public void testThatNameIsNotOverridden() {
    Map<String, Object> backend = new HashMap<>();
    backend.put("foo", "bar");
    backend.put("io.prefix.name.name", "the name");
    backend.put("io.prefix.name.k1", "v1");
    backend.put("io.prefix.name.k2", "v2");
    backend.put("io.prefix.name.k3.x", "v3");

    Config config = new DummyConfig(backend);
    Map<String, ConnectorConfig> map = ConfiguredChannelFactory.extractConfigurationFor("io.prefix", config);

    assertThat(map).hasSize(1).containsKeys("name");
    ConnectorConfig config1 = map.get("name");
    assertThat(config1.getPropertyNames()).hasSize(4);
    assertThat(config1.getValue("name", String.class)).isEqualTo("the name");
  }

  @Test
  public void testThatNameIsInject() {
    Map<String, Object> backend = new HashMap<>();
    backend.put("foo", "bar");
    backend.put("io.prefix.name.k1", "v1");
    backend.put("io.prefix.name.k2", "v2");
    backend.put("io.prefix.name.k3.x", "v3");

    Config config = new DummyConfig(backend);
    Map<String, ConnectorConfig> map = ConfiguredChannelFactory.extractConfigurationFor("io.prefix", config);

    assertThat(map).hasSize(1).containsKeys("name");
    ConnectorConfig config1 = map.get("name");
    assertThat(config1.getPropertyNames()).hasSize(4);
    assertThat(config1.getValue("channel-name", String.class)).isEqualTo("name");
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
