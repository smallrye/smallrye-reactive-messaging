package io.smallrye.reactive.messaging.test.common.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * An implementation of {@link Config} based on a simple {@link Map}.
 * This class is just use to mock real configuration, so should only be used for tests.
 * <p>
 * Note that this implementation does not do any conversion, so you must pass the expected object instances.
 */
public class MapBasedConfig extends LinkedHashMap<String, Object> implements Config {

    protected static final String TEST_MP_CFG_PROPERTIES = "target/test-classes/META-INF/microprofile-config.properties";

    public MapBasedConfig(Map<String, Object> map) {
        super(map);
    }

    public MapBasedConfig() {
        super();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void cleanup() {
        File out = new File(TEST_MP_CFG_PROPERTIES);
        if (out.isFile()) {
            out.delete();
        }
    }

    public MapBasedConfig with(String k, Object v) {
        return put(k, v);
    }

    public MapBasedConfig without(String s) {
        this.remove(s);
        return this;
    }

    public MapBasedConfig copy() {
        return new MapBasedConfig(this);
    }

    @Override
    public MapBasedConfig put(String key, Object value) {
        super.put(key, value);
        return this;
    }

    public Map<String, Object> getMap() {
        return this;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType).orElseThrow(() -> new NoSuchElementException(propertyName));
    }

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        return new ConfigValue() {
            @Override
            public String getName() {
                return propertyName;
            }

            @Override
            public String getValue() {
                return MapBasedConfig.this.getOptionalValue(propertyName, String.class).orElse(null);
            }

            @Override
            public String getRawValue() {
                return MapBasedConfig.this.getOptionalValue(propertyName, String.class).orElse(null);
            }

            @Override
            public String getSourceName() {
                return MapBasedConfig.class.getName();
            }

            @Override
            public int getSourceOrdinal() {
                return ConfigSource.DEFAULT_ORDINAL;
            }
        };
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
        return ConfigProvider.getConfig().getConverter(forType);
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        return ConfigProvider.getConfig().unwrap(type);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void write() {
        File out = new File(TEST_MP_CFG_PROPERTIES);
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
}
