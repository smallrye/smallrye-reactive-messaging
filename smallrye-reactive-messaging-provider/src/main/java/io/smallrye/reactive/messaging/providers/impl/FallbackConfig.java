package io.smallrye.reactive.messaging.providers.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Represents a configuration class that allows providing fallback values for specific properties for a connector.
 * If a property is not defined in the overall config, the fallback value is used.
 *
 * @see Config
 */
public class FallbackConfig implements Config {

    private final Config overall;
    private final Map<String, Object> fallbacks;

    public FallbackConfig(Config overall, Map<String, Object> fallbacks) {
        this.overall = overall;
        this.fallbacks = fallbacks;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType)
                .orElseThrow(() -> new java.util.NoSuchElementException("Property " + propertyName + " not found"));
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        // 1. Try the main config first
        Optional<T> value = overall.getOptionalValue(propertyName, propertyType);
        if (value.isPresent()) {
            return value;
        }
        // 2. Try override functions first
        Object o = fallbacks.get(propertyName);
        if (o != null) {
            if (propertyType.isInstance(o)) {
                return Optional.of((T) o);
            }
            if (o instanceof String) {
                return convertOptional((String) o, propertyType);
            }
            return Optional.empty();
        }

        return Optional.empty();
    }

    protected <T> Optional<T> convertOptional(String rawValue, Class<T> propertyType) {
        Optional<Converter<T>> converter = overall.getConverter(propertyType);

        if (!converter.isPresent() && propertyType.isAssignableFrom(String.class)) {
            return Optional.of(propertyType.cast(rawValue));
        }

        return converter.map(c -> c.convert(rawValue));
    }

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        return overall.getConfigValue(propertyName);
    }

    /**
     * Gets the lists of config keys for the given connector.
     * Note that the list contains property names from the config and env variables.
     * It includes keys from the connector config and channel config.
     *
     * @return the list of keys
     */
    @Override
    public Iterable<String> getPropertyNames() {
        Set<String> names = new HashSet<>(fallbacks.keySet());
        overall.getPropertyNames().forEach(names::add);
        return names;
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return overall.getConfigSources();
    }

    @Override
    public <T> Optional<Converter<T>> getConverter(Class<T> forType) {
        return overall.getConverter(forType);
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        return overall.unwrap(type);
    }

}
