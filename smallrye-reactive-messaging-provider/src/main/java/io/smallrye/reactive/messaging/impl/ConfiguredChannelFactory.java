package io.smallrye.reactive.messaging.impl;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.BeanAttributes;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.*;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PublisherDecorator;

/**
 * Look for stream factories and get instances.
 */
@ApplicationScoped
public class ConfiguredChannelFactory implements ChannelRegistar {

    private final Instance<IncomingConnectorFactory> incomingConnectorFactories;
    private final Instance<OutgoingConnectorFactory> outgoingConnectorFactories;

    protected final Config config;
    protected final ChannelRegistry registry;

    @Inject
    private Instance<PublisherDecorator> publisherDecoratorInstance;

    // CDI requirement for normal scoped beans
    protected ConfiguredChannelFactory() {
        this.incomingConnectorFactories = null;
        this.outgoingConnectorFactories = null;
        this.config = null;
        this.registry = null;
    }

    @Inject
    public ConfiguredChannelFactory(@Any Instance<IncomingConnectorFactory> incomingConnectorFactories,
            @Any Instance<OutgoingConnectorFactory> outgoingConnectorFactories,
            Instance<Config> config, @Any Instance<ChannelRegistry> registry,
            BeanManager beanManager) {

        this(incomingConnectorFactories, outgoingConnectorFactories, config, registry, beanManager, true);
    }

    ConfiguredChannelFactory(@Any Instance<IncomingConnectorFactory> incomingConnectorFactories,
            @Any Instance<OutgoingConnectorFactory> outgoingConnectorFactories,
            Instance<Config> config, @Any Instance<ChannelRegistry> registry,
            BeanManager beanManager, boolean logConnectors) {
        this.registry = registry.get();
        if (config.isUnsatisfied()) {
            this.incomingConnectorFactories = null;
            this.outgoingConnectorFactories = null;
            this.config = null;
        } else {
            this.incomingConnectorFactories = incomingConnectorFactories;
            this.outgoingConnectorFactories = outgoingConnectorFactories;
            if (logConnectors) {
                log.foundIncomingConnectors(getConnectors(beanManager, IncomingConnectorFactory.class));
                log.foundOutgoingConnectors(getConnectors(beanManager, OutgoingConnectorFactory.class));
            }
            this.config = config.stream().findFirst()
                    .orElseThrow(ex::illegalStateRetrieveConfig);
        }
    }

    private List<String> getConnectors(BeanManager beanManager, Class<?> clazz) {
        return beanManager.getBeans(clazz, Any.Literal.INSTANCE).stream()
                .map(BeanAttributes::getQualifiers)
                .flatMap(set -> set.stream().filter(a -> a.annotationType().equals(Connector.class)))
                .map(annotation -> ((Connector) annotation).value())
                .collect(Collectors.toList());
    }

    static Map<String, ConnectorConfig> extractConfigurationFor(String prefix, Config root) {
        Iterable<String> names = root.getPropertyNames();
        Map<String, ConnectorConfig> configs = new HashMap<>();
        names.forEach(key -> {
            // $prefix$name.key=value (the prefix ends with a .)
            if (key.startsWith(prefix)) {
                // Extract the name
                String name = key.substring(prefix.length());
                if (name.charAt(0) == '"') { // Check if the name is enclosed by double quotes
                    name = name.substring(1, name.lastIndexOf('"'));
                } else if (name.contains(".")) { // We must remove the part after the first dot
                    String tmp = name;
                    name = tmp.substring(0, tmp.indexOf('.'));
                }
                configs.put(name, new ConnectorConfig(prefix, root, name));
            }
        });
        return configs;
    }

    @Override
    public void initialize() {
        if (this.config == null) {
            log.skippingMPConfig();
            return;
        }

        log.channelManagerInitializing();

        Map<String, ConnectorConfig> sourceConfiguration = extractConfigurationFor(ConnectorFactory.INCOMING_PREFIX, config);
        Map<String, ConnectorConfig> sinkConfiguration = extractConfigurationFor(ConnectorFactory.OUTGOING_PREFIX, config);

        detectNameConflict(sourceConfiguration, sinkConfiguration);

        register(sourceConfiguration, sinkConfiguration);
    }

    /**
     * By spec, you cannot use the same channel name in an `incoming` configuration and `outgoing` configuration.
     * This method throws a {@link javax.enterprise.inject.spi.DeploymentException} is this case is detected.
     *
     * @param sourceConfiguration the source configurations
     * @param sinkConfiguration the sink configurations
     */
    private void detectNameConflict(Map<String, ConnectorConfig> sourceConfiguration,
            Map<String, ConnectorConfig> sinkConfiguration) {
        // We must create a copy as removing the items from the set remove them from the map.
        Set<String> sources = new HashSet<>(sourceConfiguration.keySet());
        Set<String> sinks = sinkConfiguration.keySet();
        sources.retainAll(sinks);
        if (!sources.isEmpty()) {
            throw ex.deploymentInvalidConfiguration(sources);
        }

    }

    void register(Map<String, ConnectorConfig> sourceConfiguration, Map<String, ConnectorConfig> sinkConfiguration) {
        try {
            for (Map.Entry<String, ConnectorConfig> entry : sourceConfiguration.entrySet()) {
                String channel = entry.getKey();
                ConnectorConfig config = entry.getValue();
                if (config.getOptionalValue(ConnectorConfig.CHANNEL_ENABLED_PROPERTY, Boolean.TYPE).orElse(true)) {
                    registry.register(channel, createPublisherBuilder(channel, config),
                            config.getOptionalValue(ConnectorConfig.BROADCAST_PROPERTY, Boolean.class).orElse(false));
                } else {
                    log.incomingChannelDisabled(channel);
                }
            }

            for (Map.Entry<String, ConnectorConfig> entry : sinkConfiguration.entrySet()) {
                String channel = entry.getKey();
                ConnectorConfig config = entry.getValue();
                if (config.getOptionalValue(ConnectorConfig.CHANNEL_ENABLED_PROPERTY, Boolean.TYPE).orElse(true)) {
                    registry.register(channel, createSubscriberBuilder(channel, config));
                } else {
                    log.outgoingChannelDisabled(channel);
                }
            }
        } catch (RuntimeException e) { // NOSONAR
            log.unableToCreatePublisherOrSubscriber(e);
            throw e;
        }
    }

    private static String getConnectorAttribute(Config config) {
        // This method looks for connector and type.
        // The availability has been checked when the config object has been created
        return config.getValue("connector", String.class);
    }

    private PublisherBuilder<? extends Message<?>> createPublisherBuilder(String name, Config config) {
        // Extract the type and throw an exception if missing
        String connector = getConnectorAttribute(config);

        // Look for the factory and throw an exception if missing
        IncomingConnectorFactory mySourceFactory = incomingConnectorFactories.select(ConnectorLiteral.of(connector))
                .stream().findFirst().orElseThrow(() -> ex.illegalArgumentUnknownConnector(name));

        PublisherBuilder<? extends Message<?>> publisher = mySourceFactory.getPublisherBuilder(config);

        for (PublisherDecorator decorator : publisherDecoratorInstance) {
            publisher = decorator.decorate(publisher, name);
        }

        return publisher;
    }

    private SubscriberBuilder<? extends Message<?>, Void> createSubscriberBuilder(String name, Config config) {
        // Extract the type and throw an exception if missing
        String connector = getConnectorAttribute(config);

        // Look for the factory and throw an exception if missing
        OutgoingConnectorFactory mySinkFactory = outgoingConnectorFactories.select(ConnectorLiteral.of(connector))
                .stream().findFirst().orElseThrow(() -> ex.illegalArgumentUnknownConnector(name));

        return mySinkFactory.getSubscriberBuilder(config);
    }
}
