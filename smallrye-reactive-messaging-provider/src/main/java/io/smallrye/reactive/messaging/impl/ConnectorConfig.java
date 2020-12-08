package io.smallrye.reactive.messaging.impl;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderMessages.msg;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.*;

import java.util.*;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Implementation of config used to configured the different messaging provider / connector.
 */
public class ConnectorConfig implements Config {

    /**
     * Name of the attribute checking if the channel is enabled (default) or disabled.
     * The value must be either `true` or `false`.
     */
    public static final String CHANNEL_ENABLED_PROPERTY = "enabled";

    private final String prefix;
    private final Config overall;

    private final String name;
    private final String connector;

    protected ConnectorConfig(String prefix, Config overall, String channel) {
        this.prefix = Objects.requireNonNull(prefix, msg.prefixMustNotBeSet());
        this.overall = Objects.requireNonNull(overall, msg.configMustNotBeSet());
        this.name = Objects.requireNonNull(channel, msg.channelMustNotBeSet());

        Optional<String> value = overall.getOptionalValue(channelKey(CONNECTOR_ATTRIBUTE), String.class);
        this.connector = value
                .orElseGet(() -> overall.getOptionalValue(channelKey("type"), String.class) // Legacy
                        .orElseThrow(() -> ex.illegalArgumentChannelConnectorConfiguration(name)));

        // Detect invalid channel-name attribute
        for (String key : overall.getPropertyNames()) {
            if ((channelKey(CHANNEL_NAME_ATTRIBUTE)).equalsIgnoreCase(key)) {
                throw ex.illegalArgumentInvalidChannelConfiguration(name);
            }
        }
    }

    private String channelKey(String keyName) {
        return name.contains(".") ? prefix + "\"" + name + "\"." + keyName : prefix + name + "." + keyName;
    }

    private String connectorKey(String keyName) {
        return CONNECTOR_PREFIX + connector + "." + keyName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return (T) name;
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return (T) connector;
        }

        // First check if the channel configuration contains the desired attribute.
        try {
            return overall.getValue(channelKey(propertyName), propertyType);
        } catch (NoSuchElementException e) {
            // If not, check the connector configuration
            try {
                return overall.getValue(connectorKey(propertyName), propertyType);
            } catch (NoSuchElementException e2) {
                // Catch the exception to provide a more meaningful error messages.
                throw ex.noSuchElementForAttribute(propertyName, name, channelKey(propertyName),
                        connectorKey(propertyName));
            }
        }
    }

    @Override
    public ConfigValue getConfigValue(String s) {
        ConfigValue value = overall.getConfigValue(channelKey(s));
        if (value.getRawValue() == null) {
            // Not found.
            return overall.getConfigValue(connectorKey(s));
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return Optional.of((T) name);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return Optional.of((T) connector);
        }

        // First check if the channel configuration contains the desired attribute.
        Optional<T> maybe = overall.getOptionalValue(channelKey(propertyName), propertyType);
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
    @Override
    public Iterable<String> getPropertyNames() {
        String connectorPrefix = CONNECTOR_PREFIX + connector + ".";
        String prefix = this.prefix + name + ".";
        String prefixFromEnv = toEnv(prefix);
        String connectorPrefixFromEnv = toEnv(connectorPrefix);

        Set<String> names = new HashSet<>();
        for (String name : overall.getPropertyNames()) {
            if (name.startsWith(connectorPrefix)) {
                String computed = name.substring(connectorPrefix.length());
                if (doesNotContainEnv(names, computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(connectorPrefixFromEnv)) {
                String computed = name.substring(connectorPrefixFromEnv.length());
                // Remove the potential existing key
                names.removeIf(s -> toEnv(s).equalsIgnoreCase(computed));
                names.add(computed);
            } else if (name.startsWith(prefix)) {
                String computed = name.substring(prefix.length());
                if (doesNotContainEnv(names, computed)) {
                    names.add(computed);
                }
            } else if (name.startsWith(prefixFromEnv)) {
                String computed = name.substring(prefixFromEnv.length());
                // Remove the potential existing key
                names.removeIf(s -> toEnv(s).equalsIgnoreCase(computed));
                names.add(computed);
            }
        }

        names.add(CHANNEL_NAME_ATTRIBUTE);
        return names;
    }

    private boolean doesNotContainEnv(Set<String> names, String computed) {
        String env = toEnv(computed);
        return !names.contains(env);
    }

    private String toEnv(String key) {
        return key.toUpperCase().replace(".", "_").replace("-", "_");
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return overall.getConfigSources();
    }

    @Override
    public <T> Optional<Converter<T>> getConverter(Class<T> aClass) {
        return overall.getConverter(aClass);
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return overall.unwrap(aClass);
    }
}
