package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderMessages.msg;
import static org.eclipse.microprofile.reactive.messaging.spi.ConnectorFactory.*;

import java.util.*;
import java.util.regex.Pattern;

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

    /**
     * Name of the attribute configuring the broadcast on a connector.
     */
    public static final String BROADCAST_PROPERTY = "broadcast";

    /**
     * Name of the attribute configuring the merge on a connector.
     */
    public static final String MERGE_PROPERTY = "merge";

    protected final String prefix;
    protected final Config overall;

    protected final String name;
    protected final String connector;
    protected final String channelPrefix;

    protected ConnectorConfig(String prefix, Config overall, String connector, String channel) {
        this.prefix = Objects.requireNonNull(prefix, msg.prefixMustNotBeSet());
        this.overall = Objects.requireNonNull(overall, msg.configMustNotBeSet());
        this.name = Objects.requireNonNull(channel, msg.channelMustNotBeSet());
        this.channelPrefix = channelPrefix(prefix, name);
        this.connector = connector;

        // Detect invalid channel-name attribute
        for (String key : overall.getPropertyNames()) {
            if ((channelKey(CHANNEL_NAME_ATTRIBUTE)).equalsIgnoreCase(key)) {
                throw ex.illegalArgumentInvalidChannelConfiguration(name);
            }
        }
    }

    public ConnectorConfig(String prefix, Config overall, String channel) {
        this(prefix, overall, getConnectorAttribute(prefix, overall, channel), channel);
    }

    public static String getConnectorAttribute(String prefix, Config overall, String channel) {
        return overall.getOptionalValue(channelPrefix(prefix, channel) + CONNECTOR_ATTRIBUTE, String.class)
                .orElseGet(() -> overall.getOptionalValue(channelPrefix(prefix, channel) + "type", String.class) // Legacy
                        .orElseThrow(() -> ex.illegalArgumentChannelConnectorConfiguration(channel)));
    }

    public static String channelPrefix(String prefix, String name) {
        return name.contains(".") ? prefix + "\"" + name + "\"." : prefix + name + ".";
    }

    protected String channelKey(String keyName) {
        return channelPrefix + keyName;
    }

    protected String connectorKey(String keyName) {
        return CONNECTOR_PREFIX + connector + "." + keyName;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return convert(name, propertyType);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return convert(connector, propertyType);
        }

        // First check if the channel configuration contains the desired attribute.
        Optional<T> maybeResult = overall.getOptionalValue(channelKey(propertyName), propertyType);
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
            return new ConfigValueImpl(CHANNEL_NAME_ATTRIBUTE, name);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return new ConfigValueImpl(CONNECTOR_ATTRIBUTE, connector);
        }
        // First check if the channel configuration contains the desired attribute.
        ConfigValue value = overall.getConfigValue(channelKey(propertyName));
        if (value.getRawValue() == null) {
            // Try connector configuration
            return overall.getConfigValue(connectorKey(propertyName));
        }
        return value;
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        if (CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(propertyName)) {
            return convertOptional(name, propertyType);
        }
        if (CONNECTOR_ATTRIBUTE.equalsIgnoreCase(propertyName) || "type".equalsIgnoreCase(propertyName)) {
            return convertOptional(connector, propertyType);
        }

        // First check if the channel configuration contains the desired attribute.
        Optional<T> maybe = overall.getOptionalValue(channelKey(propertyName), propertyType);
        return maybe.isPresent() ? maybe
                : overall.getOptionalValue(connectorKey(propertyName), propertyType);
    }

    protected <T> T convert(String rawValue, Class<T> propertyType) {
        Optional<Converter<T>> converter = overall.getConverter(propertyType);

        if (!converter.isPresent()) {
            if (propertyType.isAssignableFrom(String.class)) {
                return propertyType.cast(rawValue);
            }
            throw ex.noConverterForType(propertyType);
        }

        T result = converter.get().convert(rawValue);

        if (result == null) {
            throw ex.converterReturnedNull(converter.get(), rawValue);
        }

        return result;
    }

    protected <T> Optional<T> convertOptional(String rawValue, Class<T> propertyType) {
        Optional<Converter<T>> converter = overall.getConverter(propertyType);

        if (!converter.isPresent() && propertyType.isAssignableFrom(String.class)) {
            return Optional.of(propertyType.cast(rawValue));
        }

        return converter.map(c -> c.convert(rawValue));
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
        String prefixAlpha = toAlpha(prefix);
        String prefixAlphaUpper = prefixAlpha.toUpperCase();
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
        if (type.isInstance(this)) {
            return type.cast(this);
        } else {
            throw ex.configNotOfType(type);
        }
    }

    protected static class ConfigValueImpl implements ConfigValue {

        private final String name;
        private final String value;

        public ConfigValueImpl(String name, String value) {
            super();
            this.name = name;
            this.value = value;
        }

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
            return "ConnectorConfig internal";
        }

        @Override
        public int getSourceOrdinal() {
            return 0;
        }

    }
}
