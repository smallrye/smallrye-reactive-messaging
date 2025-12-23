package io.smallrye.reactive.messaging.providers.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Represents a configuration class that allows overriding specific properties for a connector.
 * If an override is defined for a property, it is used first.
 * Otherwise, the lookup is delegated to the overall config.
 *
 * @see Config
 */
public class OverrideConfig implements Config {

    private final Config overall;
    private final Map<String, Function<OverrideConfig, Object>> overrides;

    public OverrideConfig(Config overall, Map<String, Function<OverrideConfig, Object>> overrides) {
        this.overall = overall;
        this.overrides = overrides;
    }

    /**
     * Access the original value from the delegate without overrides or nested channel logic.
     */
    public <T> Optional<T> getOriginalValue(String propertyName, Class<T> propertyType) {
        return overall.getOptionalValue(propertyName, propertyType);
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType)
                .orElseThrow(() -> new java.util.NoSuchElementException("Property " + propertyName + " not found"));
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        // 1. Try override functions first
        Function<OverrideConfig, Object> function = overrides.get(propertyName);
        if (function != null) {
            Object o = function.apply(this);
            if (o != null) {
                if (propertyType.isInstance(o)) {
                    return Optional.of((T) o);
                }
                if (o instanceof String) {
                    return convertOptional((String) o, propertyType);
                }
                return Optional.empty();
            }
        }

        // 3. Delegate to wrapped ConnectorConfig
        return overall.getOptionalValue(propertyName, propertyType);
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
        Function<OverrideConfig, Object> function = overrides.get(propertyName);

        if (function != null) {
            Object o = function.apply(this);
            if (o != null) {
                return new ConnectorConfig.ConfigValueImpl(propertyName, String.valueOf(o));
            }
        }
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
        Set<String> names = new HashSet<>();
        for (String name : overall.getPropertyNames()) {
            names.add(name);
        }

        names.addAll(overrides.keySet());
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
