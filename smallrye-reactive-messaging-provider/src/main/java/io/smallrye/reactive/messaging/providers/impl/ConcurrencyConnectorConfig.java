package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CHANNEL_NAME_ATTRIBUTE;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CONNECTOR_ATTRIBUTE;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.CONNECTOR_PREFIX;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.INCOMING_PREFIX;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;

/**
 * Implementation of config used to configured the different messaging provider / connector.
 */
public class ConcurrencyConnectorConfig extends ConnectorConfig {

    /**
     * Name of the attribute configuring the concurrency on a connector.
     */
    public static final String CONCURRENCY_PROPERTY_KEY = "concurrency";

    /**
     * Separator char for distinguishing per concurrent channel copy names
     */
    public static final String CONCURRENCY_CONFIG_SEPARATOR = "$";

    public static Optional<Integer> getConcurrency(Config connectorConfig) {
        return connectorConfig.getOptionalValue(CONCURRENCY_PROPERTY_KEY, Integer.class);
    }

    public static Optional<Integer> getConcurrency(String channel, Config rootConfig) {
        String configKey = channelPrefix(INCOMING_PREFIX, channel) + CONCURRENCY_PROPERTY_KEY;
        return rootConfig.getOptionalValue(configKey, Integer.class);
    }

    public static String stripChannelNameOfSeparator(String name) {
        return name.substring(0, name.indexOf(CONCURRENCY_CONFIG_SEPARATOR));
    }

    public static boolean isConcurrencyChannelName(String name) {
        return name.contains(CONCURRENCY_CONFIG_SEPARATOR);
    }

    private final String indexedChannelPrefix;
    private final String indexedChannel;

    public ConcurrencyConnectorConfig(String prefix, Config overall, String channel, int index) {
        super(prefix, overall, channel);
        this.indexedChannel = channel + CONCURRENCY_CONFIG_SEPARATOR + index;
        this.indexedChannelPrefix = channelPrefix(prefix, indexedChannel);
    }

    public String getIndexedChannel() {
        return indexedChannel;
    }

    public ConcurrencyConnectorConfig(ConnectorConfig config, int index) {
        this(config.prefix, config.overall, config.name, index);
    }

    public String indexedChannelKey(String propertyName) {
        return indexedChannelPrefix + propertyName;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return convert(indexedChannel, propertyType);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return convert(connector, propertyType);
        }
        // First check if the indexed channel configuration contains the desired attribute.
        Optional<T> maybeResult = overall.getOptionalValue(indexedChannelKey(propertyName), propertyType);
        if (maybeResult.isPresent()) {
            return maybeResult.get();
        }

        // Second check if the channel configuration contains the desired attribute.
        maybeResult = overall.getOptionalValue(channelKey(propertyName), propertyType);
        if (maybeResult.isPresent()) {
            return maybeResult.get();
        }

        // Then check if the connector configuration contains the desired attribute.
        maybeResult = overall.getOptionalValue(connectorKey(propertyName), propertyType);
        if (maybeResult.isPresent()) {
            return maybeResult.get();
        }

        throw ex.noSuchElementForAttribute(propertyName, name, channelKey(propertyName), connectorKey(propertyName));
    }

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return new ConfigValueImpl(CHANNEL_NAME_ATTRIBUTE, indexedChannel);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return new ConfigValueImpl(CONNECTOR_ATTRIBUTE, connector);
        }
        // First check if the indexed channel configuration contains the desired attribute.
        ConfigValue value = overall.getConfigValue(indexedChannelKey(propertyName));
        if (value.getRawValue() != null) {
            return value;
        }
        // Second check if the channel configuration contains the desired attribute.
        value = overall.getConfigValue(channelKey(propertyName));
        if (value.getRawValue() == null) {
            // Try connector configuration
            return overall.getConfigValue(connectorKey(propertyName));
        }
        return value;
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return convertOptional(indexedChannel, propertyType);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return convertOptional(connector, propertyType);
        }
        // First check if the indexed channel configuration contains the desired attribute.
        Optional<T> maybe = overall.getOptionalValue(indexedChannelKey(propertyName), propertyType);
        if (maybe.isPresent()) {
            return maybe;
        }

        // First check if the channel configuration contains the desired attribute.
        maybe = overall.getOptionalValue(channelKey(propertyName), propertyType);
        return maybe.isPresent() ? maybe
                : overall.getOptionalValue(connectorKey(propertyName), propertyType);
    }

    /**
     * Gets the lists of config keys for the given connector.
     * Note that the list contains property names from the config and env variables.
     * It includes keys from the connector config and channel config.
     *
     * @return the list of keys
     */
    public Iterable<String> getPropertyNames() {
        String prefix = channelPrefix;
        String prefixAlpha = toAlpha(prefix);
        String prefixAlphaUpper = prefixAlpha.toUpperCase();
        String indexedPrefix = indexedChannelPrefix;
        String indexedPrefixAlpha = toAlpha(indexedPrefix);
        String indexedPrefixAlphaUpper = indexedPrefixAlpha.toUpperCase();
        String connectorPrefix = CONNECTOR_PREFIX + connector + ".";
        String connectorPrefixAlpha = toAlpha(connectorPrefix);
        String connectorPrefixAlphaUpper = connectorPrefixAlpha.toUpperCase();

        Set<String> names = new HashSet<>();
        for (String name : overall.getPropertyNames()) {
            if (name.startsWith(connectorPrefix)) {
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
            } else if (name.startsWith(indexedPrefix)) {
                String computed = name.substring(indexedPrefix.length());
                names.add(computed);
            } else if (name.startsWith(indexedPrefixAlpha)) {
                String computed = name.substring(indexedPrefixAlpha.length());
                if (nameExists(indexedPrefix + computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(indexedPrefixAlphaUpper)) {
                String computed = name.substring(indexedPrefixAlphaUpper.length());
                if (nameExists(indexedPrefix + computed)) {
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
            }
        }

        names.add(CHANNEL_NAME_ATTRIBUTE);
        return names;
    }

}
