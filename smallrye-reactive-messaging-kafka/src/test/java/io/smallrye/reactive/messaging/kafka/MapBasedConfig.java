package io.smallrye.reactive.messaging.kafka;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * An implementation of {@link Config} based on a simple {@link Map}.
 * This class is just use to mock real configuration, so should only be used for tests.
 * <p>
 * Note that this implementation does not do any conversion, so you must pass the expected object instances.
 */
public class MapBasedConfig implements Config {
    private final Map<String, Object> map;

    public MapBasedConfig(Map<String, Object> map) {
        this.map = map;
    }

    public MapBasedConfig() {
        this.map = new HashMap<>();
    }

    public MapBasedConfig put(String k, Object v) {
        this.map.put(k, v);
        return this;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void clear() {
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
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        @SuppressWarnings("unchecked")
        T value = (T) map.get(propertyName);
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void write() {
        File out = new File("target/test-classes/META-INF/microprofile-config.properties");
        if (out.isFile()) {
            out.delete();
        }
        out.getParentFile().mkdirs();

        Properties properties = new Properties();
        map.forEach((key, value) -> properties.setProperty(key, value.toString()));
        try (FileOutputStream fos = new FileOutputStream(out)) {
            properties.store(fos, "file generated for testing purpose");
            fos.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class ConfigBuilder {
        private final String prefix;
        private final Boolean withTracing;
        private final Map<String, Object> configValues = new HashMap<>();

        public ConfigBuilder(String prefix) {
            this(prefix, false);
        }

        public ConfigBuilder(String prefix, Boolean withTracing) {
            this.prefix = prefix;
            this.withTracing = withTracing;
        }

        public ConfigBuilder put(String key, Object value) {
            configValues.put(key, value);
            return this;
        }

        private String getFullKey(String shortKey) {
            return prefix + "." + shortKey;
        }

        public Map<String, Object> build() {
            Map<String, Object> config = new HashMap<>();
            if (!withTracing && !configValues.containsKey("tracing-enabled")) {
                config.put(getFullKey("tracing-enabled"), false);
            }
            configValues.forEach((key, value) -> config.put(getFullKey(key), value));
            return config;
        }
    }
}
