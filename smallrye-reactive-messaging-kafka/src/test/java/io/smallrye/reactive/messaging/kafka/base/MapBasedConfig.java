package io.smallrye.reactive.messaging.kafka.base;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;

/**
 * An implementation of {@link Config} based on a simple {@link Map}.
 * This class is just use to mock real configuration, so should only be used for tests.
 * <p>
 * Note that this implementation does not do any conversion, so you must pass the expected object instances.
 */
public class MapBasedConfig extends HashMap<String, Object> implements Config, Map<String, Object> {

    public MapBasedConfig(Map<String, Object> map) {
        super(map);
    }

    public MapBasedConfig() {
        super();
    }

    public static Builder builder() {
        return builder("");
    }

    public static Builder builder(String prefix) {
        return builder(prefix, false);
    }

    public static Builder builder(String prefix, boolean tracing) {
        return new MapBasedConfig.Builder(prefix, tracing);
    }

    public MapBasedConfig with(String k, Object v) {
        super.put(k, v);
        return this;
    }

    public MapBasedConfig without(String s) {
        super.remove(s);
        return this;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void cleanup() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType).orElseThrow(() -> new NoSuchElementException(propertyName));
    }

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        throw new UnsupportedOperationException("don't call this method");
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        @SuppressWarnings("unchecked")
        T value = (T) super.get(propertyName);
        return Optional.ofNullable(value);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return super.keySet();
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
        throw new UnsupportedOperationException("don't call this method");
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void write() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
        out.getParentFile().mkdirs();

        Properties properties = new Properties();
        super.forEach((key, value) -> properties.setProperty(key, value.toString()));
        try (FileOutputStream fos = new FileOutputStream(out)) {
            properties.store(fos, "file generated for testing purpose");
            fos.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public MapBasedConfig copy() {
        return new MapBasedConfig(new HashMap<>(this));
    }

    public static class Builder {
        private final String prefix;
        private final Boolean withTracing;
        private final Map<String, Object> configValues = new HashMap<>();

        private Builder(String prefix, Boolean withTracing) {
            this.prefix = prefix;
            this.withTracing = withTracing;
        }

        public Builder put(String key, Object value) {
            configValues.put(key, value);
            return this;
        }

        public Builder put(Object... keyOrValue) {
            String k = null;
            for (Object o : keyOrValue) {
                if (k == null) {
                    if (o instanceof String) {
                        k = o.toString();
                    } else {
                        throw new IllegalArgumentException("Expected " + o + " to be a String");
                    }
                } else {
                    put(k, o);
                    k = null;
                }
            }
            if (k != null) {
                throw new IllegalArgumentException("Invalid number of parameters, last key " + k + " has no value");
            }
            return this;
        }

        private String getFullKey(String shortKey) {
            if (prefix.length() > 0) {
                return prefix + "." + shortKey;
            } else {
                return shortKey;
            }
        }

        public MapBasedConfig build() {
            Map<String, Object> inner = new HashMap<>();

            if (!configValues.containsKey("connector")) {
                inner.put(getFullKey("connector"), KafkaConnector.CONNECTOR_NAME);
            }

            if (!withTracing && !configValues.containsKey("tracing-enabled")) {
                inner.put(getFullKey("tracing-enabled"), false);
            }
            if (!configValues.containsKey("bootstrap.servers")) {
                inner.put(getFullKey("bootstrap.servers"), KafkaTestBase.getBootstrapServers());
            }
            configValues.forEach((key, value) -> inner.put(getFullKey(key), value));
            return new MapBasedConfig(inner);
        }
    }
}
