package io.smallrye.reactive.messaging.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;
import org.junit.Test;

public class ConfiguredStreamFactoryTest {

    @Test
    public void testSimpleExtraction() {
        Map<String, Object> backend = new HashMap<>();
        backend.put("foo", "bar");
        backend.put("io.prefix.name.type", "my-connector"); // Legacy
        backend.put("io.prefix.name.k1", "v1");
        backend.put("io.prefix.name.k2", "v2");
        backend.put("io.prefix.name.k3.x", "v3");

        backend.put("io.prefix.name2.connector", "my-connector");
        backend.put("io.prefix.name2.k1", "v1");
        backend.put("io.prefix.name2.k2", "v2");
        backend.put("io.prefix.name2.k3.x", "boo");
        backend.put("io.prefix.name2.another", "1");

        Config config = new DummyConfig(backend);
        Map<String, ConnectorConfig> map = ConfiguredChannelFactory.extractConfigurationFor("io.prefix.", config);

        assertThat(map).hasSize(2).containsKeys("name", "name2");
        ConnectorConfig config1 = map.get("name");
        ConnectorConfig config2 = map.get("name2");
        assertThat(config1.getPropertyNames()).hasSize(5).contains("k1", "k2", "k3.x", "channel-name", "type");
        assertThat(config1.getValue("k1", String.class)).isEqualTo("v1");
        assertThat(config1.getValue("k2", String.class)).isEqualTo("v2");
        assertThat(config1.getValue("k3.x", String.class)).isEqualTo("v3");
        assertThat(config1.getValue("channel-name", String.class)).isEqualTo("name");
        assertThat(config2.getPropertyNames()).hasSize(6).contains("k1", "k2", "k3.x", "another", "channel-name", "connector");
        assertThat(config2.getValue("k1", String.class)).isEqualTo("v1");
        assertThat(config2.getValue("k2", String.class)).isEqualTo("v2");
        assertThat(config2.getValue("k3.x", String.class)).isEqualTo("boo");
        assertThat(config2.getValue("channel-name", String.class)).isEqualTo("name2");
        assertThat(config2.getOptionalValue("another", String.class)).contains("1");
        assertThat(config2.getOptionalValue("missing", String.class)).isEmpty();
    }

    @Test
    public void testThatChannelNameIsInjected() {
        Map<String, Object> backend = new HashMap<>();
        backend.put("foo", "bar");
        backend.put("io.prefix.name.connector", "my-connector");
        backend.put("io.prefix.name.k1", "v1");
        backend.put("io.prefix.name.k2", "v2");
        backend.put("io.prefix.name.k3.x", "v3");

        Config config = new DummyConfig(backend);
        Map<String, ConnectorConfig> map = ConfiguredChannelFactory.extractConfigurationFor("io.prefix.", config);
        assertThat(map).hasSize(1).containsKeys("name");
        ConnectorConfig config1 = map.get("name");
        assertThat(config1.getPropertyNames()).hasSize(5);
        assertThat(config1.getValue("channel-name", String.class)).isEqualTo("name");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatMissingConnectorAttributeIsDetected() {
        Map<String, Object> backend = new HashMap<>();
        backend.put("foo", "bar");
        backend.put("io.prefix.name.k1", "v1");
        backend.put("io.prefix.name.k2", "v2");
        backend.put("io.prefix.name.k3.x", "v3");

        Config config = new DummyConfig(backend);
        ConfiguredChannelFactory.extractConfigurationFor("io.prefix.", config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatChannelNameIsDetected() {
        Map<String, Object> backend = new HashMap<>();
        backend.put("foo", "bar");
        backend.put("io.prefix.name.connector", "my-connector");
        backend.put("io.prefix.name.k1", "v1");
        backend.put("io.prefix.name.channel-name", "v2");
        backend.put("io.prefix.name.k3.x", "v3");

        Config config = new DummyConfig(backend);
        ConfiguredChannelFactory.extractConfigurationFor("io.prefix.", config);
    }

    @Test
    public void testConnectorConfigurationLookup() {
        Map<String, Object> backend = new HashMap<>();
        backend.put("mp.messaging.connector.my-connector.a", "A");
        backend.put("mp.messaging.connector.my-connector.b", "B");

        backend.put("io.prefix.name.connector", "my-connector");
        backend.put("io.prefix.name.k1", "v1");
        backend.put("io.prefix.name.k2", "v2");
        backend.put("io.prefix.name.b", "B2");

        backend.put("io.prefix.name2.connector", "my-connector");
        backend.put("io.prefix.name2.k1", "v12");
        backend.put("io.prefix.name2.k2", "v22");
        backend.put("io.prefix.name2.b", "B22");

        Config config = new DummyConfig(backend);
        Map<String, ConnectorConfig> map = ConfiguredChannelFactory.extractConfigurationFor("io.prefix.", config);
        assertThat(map).hasSize(2).containsKeys("name", "name2");
        ConnectorConfig config1 = map.get("name");
        assertThat(config1.getPropertyNames()).hasSize(6).contains("a", "b", "k1", "k2", "connector", "channel-name");
        assertThat(config1.getValue("k1", String.class)).isEqualTo("v1");
        assertThat(config1.getValue("a", String.class)).isEqualTo("A");
        assertThat(config1.getValue("b", String.class)).isEqualTo("B2");
        assertThat(config1.getOptionalValue("k1", String.class)).contains("v1");
        assertThat(config1.getOptionalValue("a", String.class)).contains("A");
        assertThat(config1.getOptionalValue("b", String.class)).contains("B2");
        assertThat(config1.getOptionalValue("c", String.class)).isEmpty();

        ConnectorConfig config2 = map.get("name2");
        assertThat(config2.getPropertyNames()).hasSize(6).contains("a", "b", "k1", "k2", "connector", "channel-name");
        assertThat(config2.getValue("k1", String.class)).isEqualTo("v12");
        assertThat(config2.getValue("a", String.class)).isEqualTo("A");
        assertThat(config2.getValue("b", String.class)).isEqualTo("B22");
    }

    private static class DummyConfig implements Config {

        private final Map<String, Object> backend;

        private DummyConfig(Map<String, Object> backend) {
            this.backend = backend;
        }

        @Override
        public <T> T getValue(String s, Class<T> aClass) {
            return getOptionalValue(s, aClass).orElseThrow(() -> new NoSuchElementException("Key not found: " + s));
        }

        @Override
        public ConfigValue getConfigValue(String propertyName) {
            throw new UnsupportedOperationException();
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

        @Override
        public <T> Optional<Converter<T>> getConverter(Class<T> forType) {
            return Optional.empty();
        }

        @Override
        public <T> T unwrap(Class<T> type) {
            throw new UnsupportedOperationException();
        }
    }

}
