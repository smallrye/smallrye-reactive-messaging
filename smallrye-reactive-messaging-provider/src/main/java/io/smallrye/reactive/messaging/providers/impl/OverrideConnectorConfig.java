package io.smallrye.reactive.messaging.providers.impl;

import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CHANNEL_NAME_ATTRIBUTE;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CONNECTOR_PREFIX;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.eclipse.microprofile.config.Config;

/**
 * Represents a configuration class that allows overriding specific properties for a connector.
 * If a nested-channel is defined, property lookups will be in the following order
 * <ol>
 * <li>If nested-channel is given try [prefix].[channel].[nested-channel].[property-key]</li>
 * <li>If an override function is given for a property-key, calls it converts the return value</li>
 * <li>Calls the {@link ConnectorConfig} base function</li>
 * </ol>
 * The function {@link #getOriginalValue(String, Class)} allows accessing the connector config without override.
 *
 * @see ConnectorConfig
 */
public class OverrideConnectorConfig extends ConnectorConfig {

    private final String nestedChannel;
    private final Map<String, Function<OverrideConnectorConfig, Object>> overrides;

    public OverrideConnectorConfig(String prefix, Config overall, String connector, String channel,
            Map<String, Function<OverrideConnectorConfig, Object>> overrides) {
        this(prefix, overall, connector, channel, null, overrides);
    }

    public OverrideConnectorConfig(String prefix, Config overall, String connector, String channel, String nestedChannel) {
        this(prefix, overall, connector, channel, nestedChannel, new HashMap<>());
    }

    public OverrideConnectorConfig(String prefix, Config overall, String connector, String channel, String nestedChannel,
            Map<String, Function<OverrideConnectorConfig, Object>> overrides) {
        super(prefix, overall, connector, channel);
        this.nestedChannel = nestedChannel;
        this.overrides = overrides;
    }

    protected String nestedChannelKey(String keyName) {
        return nestedChannel == null ? keyName : nestedChannel + "." + keyName;
    }

    public <T> Optional<T> getOriginalValue(String propertyName, Class<T> propertyType) {
        return super.getOptionalValue(propertyName, propertyType);
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        if (nestedChannel != null) {
            // First check if the nestedChannel channel configuration contains the desired attribute.
            Optional<T> maybeResult = getOptionalValueFromSuper(nestedChannelKey(propertyName), propertyType);
            if (maybeResult.isPresent()) {
                return maybeResult.get();
            }
        }
        Function<OverrideConnectorConfig, Object> function = overrides.get(propertyName);
        if (function != null) {
            Object o = function.apply(this);
            if (o != null) {
                if (propertyType.isInstance(o)) {
                    return (T) o;
                }
                if (o instanceof String) {
                    return convert(((String) o), propertyType);
                }
            }
        }
        return getValueFromSuper(propertyName, propertyType);
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        if (nestedChannel != null) {
            // First check if the nestedChannel channel configuration contains the desired attribute.
            Optional<T> maybe = getOptionalValueFromSuper(nestedChannelKey(propertyName), propertyType);
            if (maybe.isPresent()) {
                return maybe;
            }
        }
        Function<OverrideConnectorConfig, Object> function = overrides.get(propertyName);
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
        return getOptionalValueFromSuper(propertyName, propertyType);
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
        String prefix = channelPrefix;
        String nestedPrefix = channelPrefix + nestedChannel + ".";
        String prefixAlpha = toAlpha(prefix);
        String nestedPrefixAlpha = toAlpha(nestedPrefix);
        String prefixAlphaUpper = prefixAlpha.toUpperCase();
        String nestedPrefixAlphaUpper = nestedPrefixAlpha.toUpperCase();
        String connectorPrefix = CONNECTOR_PREFIX + connector + ".";
        String connectorNestedPrefix = connectorPrefix + nestedChannel + ".";
        String connectorPrefixAlpha = toAlpha(connectorPrefix);
        String connectorNestedPrefixAlpha = toAlpha(connectorNestedPrefix);
        String connectorPrefixAlphaUpper = connectorPrefixAlpha.toUpperCase();
        String connectorNestedPrefixAlphaUpper = connectorNestedPrefixAlpha.toUpperCase();

        Set<String> names = new HashSet<>();
        for (String name : overall.getPropertyNames()) {
            if (name.startsWith(connectorNestedPrefix)) {
                String computed = name.substring(connectorNestedPrefix.length());
                names.add(computed);
            } else if (name.startsWith(connectorNestedPrefixAlpha)) {
                String computed = name.substring(connectorNestedPrefixAlpha.length());
                if (nameExists(connectorNestedPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(connectorNestedPrefixAlphaUpper)) {
                String computed = name.substring(connectorNestedPrefixAlphaUpper.length());
                if (nameExists(connectorNestedPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(connectorPrefix)) {
                String computed = name.substring(connectorPrefix.length());
                names.add(computed);
            } else if (name.startsWith(connectorPrefixAlpha)) {
                String computed = name.substring(connectorPrefixAlpha.length());
                if (nameExists(connectorPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(connectorPrefixAlphaUpper)) {
                String computed = name.substring(connectorPrefixAlphaUpper.length());
                if (nameExists(connectorPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(nestedPrefix)) {
                String computed = name.substring(nestedPrefix.length());
                names.add(computed);
            } else if (name.startsWith(nestedPrefixAlpha)) {
                String computed = name.substring(nestedPrefixAlpha.length());
                if (nameExists(nestedPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(nestedPrefixAlphaUpper)) {
                String computed = name.substring(nestedPrefixAlphaUpper.length());
                if (nameExists(nestedPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(prefix)) {
                String computed = name.substring(prefix.length());
                names.add(computed);
            } else if (name.startsWith(prefixAlpha)) {
                String computed = name.substring(prefixAlpha.length());
                if (nameExists(prefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(prefixAlphaUpper)) {
                String computed = name.substring(prefixAlphaUpper.length());
                if (nameExists(prefix + computed)) {
                    names.add(computed);
                }
            } else if (overall instanceof ConnectorConfig) {
                names.add(name);
            }
        }

        names.add(CHANNEL_NAME_ATTRIBUTE);
        names.addAll(overrides.keySet());
        return names;
    }

    private <T> Optional<T> getOptionalValueFromSuper(String propertyName, Class<T> propertyType) {
        return overall instanceof ConnectorConfig ? overall.getOptionalValue(propertyName, propertyType)
                : super.getOptionalValue(propertyName, propertyType);
    }

    private <T> T getValueFromSuper(String propertyName, Class<T> propertyType) {
        return overall instanceof ConnectorConfig ? overall.getValue(propertyName, propertyType)
                : super.getValue(propertyName, propertyType);
    }

}
