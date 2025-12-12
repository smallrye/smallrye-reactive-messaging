package io.smallrye.reactive.messaging.providers.impl;

import java.util.*;
import java.util.regex.Pattern;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Represents a configuration class that allows overriding specific properties for a connector.
 * If a nested-channel is defined, property lookups will be in the following order
 * <ol>
 * <li>If nested-channel is given try [prefix].[channel].[nested-channel].[property-key]</li>
 * <li>If an override function is given for a property-key, calls it converts the return value</li>
 * <li>Calls the {@link ConnectorConfig} base function</li>
 * </ol>
 *
 * @see ConnectorConfig
 */
public class PrefixedConfig implements Config {

    private final Config overall;
    private final String prefix;

    public static PrefixedConfig prefixed(String prefix, Config overall) {
        return new PrefixedConfig(overall, prefix);
    }

    public PrefixedConfig(Config overall, String prefix) {
        this.overall = overall;
        this.prefix = prefix;
    }

    protected String nestedChannelKey(String keyName) {
        return prefix == null ? keyName : prefix + "." + keyName;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return getOptionalValue(propertyName, propertyType)
                .orElseThrow(() -> new java.util.NoSuchElementException("Property " + propertyName + " not found"));
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        // 1. Try nested channel lookup first (e.g., "dead-letter-queue.topic")
        Optional<T> nested = overall.getOptionalValue(nestedChannelKey(propertyName), propertyType);
        if (nested.isPresent()) {
            return nested;
        }

        // 2. Delegate to wrapped ConnectorConfig
        return overall.getOptionalValue(propertyName, propertyType);
    }

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        // First check if the channel configuration contains the desired attribute.
        ConfigValue value = overall.getConfigValue(nestedChannelKey(propertyName));
        if (value.getRawValue() == null) {
            // Try connector configuration
            return overall.getConfigValue(propertyName);
        }
        return value;
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
        String nested = prefix + ".";
        String nestedAlpha = toAlpha(nested);
        String nestedAlphaUpper = nestedAlpha.toUpperCase();

        Set<String> names = new HashSet<>();
        for (String name : overall.getPropertyNames()) {
            if (name.startsWith(nested)) {
                String computed = name.substring(nested.length());
                names.add(computed);
            } else if (name.startsWith(nestedAlpha)) {
                String computed = name.substring(nestedAlpha.length());
                if (nameExists(nestedAlpha + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(nestedAlphaUpper)) {
                String computed = name.substring(nestedAlphaUpper.length());
                if (nameExists(nestedAlphaUpper + computed)) {
                    names.add(computed);
                }
            } else {
                names.add(name);
            }
        }

        return names;
    }

    private static final Pattern NON_ALPHA = Pattern.compile("\\W");

    protected String toAlpha(String key) {
        return NON_ALPHA.matcher(key).replaceAll("_");
    }

    protected boolean nameExists(String name) {
        return overall.getConfigValue(name).getRawValue() != null;
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
