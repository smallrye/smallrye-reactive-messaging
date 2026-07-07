package io.smallrye.reactive.messaging.connector;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * A simple {@link Config} implementation backed by a {@link Map}.
 * Used by generated configuration builders to create configuration objects programmatically.
 */
public class MapBasedConfig implements Config {

    private final Map<String, String> values;

    public MapBasedConfig(Map<String, String> values) {
        this.values = Map.copyOf(values);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType)
                .orElseThrow(() -> new NoSuchElementException("Property " + propertyName + " not found"));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        String value = values.get(propertyName);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of((T) convert(value, propertyType));
    }

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        String value = values.get(propertyName);
        return new SimpleConfigValue(propertyName, value);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return values.keySet();
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
        if (type.isInstance(this)) {
            return type.cast(this);
        }
        throw new IllegalArgumentException("Cannot unwrap to " + type);
    }

    private static Object convert(String value, Class<?> type) {
        if (type == String.class) {
            return value;
        }
        if (type == Boolean.class || type == boolean.class) {
            return Boolean.valueOf(value);
        }
        if (type == Integer.class || type == int.class) {
            return Integer.valueOf(value);
        }
        if (type == Long.class || type == long.class) {
            return Long.valueOf(value);
        }
        if (type == Double.class || type == double.class) {
            return Double.valueOf(value);
        }
        if (type == Float.class || type == float.class) {
            return Float.valueOf(value);
        }
        if (type == Short.class || type == short.class) {
            return Short.valueOf(value);
        }
        if (type == Byte.class || type == byte.class) {
            return Byte.valueOf(value);
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private record SimpleConfigValue(String name, String value) implements ConfigValue {

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getRawValue() {
            return value;
        }

        @Override
        public String getSourceName() {
            return "MapBasedConfig";
        }

        @Override
        public int getSourceOrdinal() {
            return 0;
        }
    }
}
