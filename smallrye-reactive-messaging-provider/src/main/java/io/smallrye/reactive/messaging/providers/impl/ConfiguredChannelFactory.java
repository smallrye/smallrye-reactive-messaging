package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import java.util.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.*;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.PublisherDecorator;

/**
 * Look for stream factories and get instances.
 */
@ApplicationScoped
public class ConfiguredChannelFactory implements ChannelRegistar {

    protected final Config config;
    protected final ChannelRegistry registry;
    private final ConnectorFactories factories;

    @Inject
    private Instance<PublisherDecorator> publisherDecoratorInstance;

    // CDI requirement for normal scoped beans
    protected ConfiguredChannelFactory() {
        this.config = null;
        this.registry = null;
        this.factories = null;
    }

    @Inject
    public ConfiguredChannelFactory(ConnectorFactories factories,
            Instance<Config> config,
            @Any Instance<ChannelRegistry> registry) {

        this(factories, config, registry, true);
    }

    ConfiguredChannelFactory(ConnectorFactories factories,
            Instance<Config> config, @Any Instance<ChannelRegistry> registry,
            boolean logConnectors) {
        this.registry = registry.get();
        this.factories = factories;
        if (config.isUnsatisfied()) {
            this.config = null;
        } else {
            if (logConnectors) {
                log.foundIncomingConnectors(factories.getInboundConnectors().keySet());
                log.foundOutgoingConnectors(factories.getOutboundConnectors().keySet());
            }
            this.config = config.stream().findFirst()
                    .orElseThrow(ex::illegalStateRetrieveConfig);
        }
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
     * This method throws a {@link jakarta.enterprise.inject.spi.DeploymentException} is this case is detected.
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

    void register(Map<String, ConnectorConfig> incomings, Map<String, ConnectorConfig> outgoings) {
        try {
            for (Map.Entry<String, ConnectorConfig> entry : incomings.entrySet()) {
                String channel = entry.getKey();
                ConnectorConfig config = entry.getValue();
                if (config.getOptionalValue(ConnectorConfig.CHANNEL_ENABLED_PROPERTY, Boolean.TYPE).orElse(true)) {
                    registry.register(channel, createPublisher(channel, config),
                            config.getOptionalValue(ConnectorConfig.BROADCAST_PROPERTY, Boolean.class).orElse(false));
                } else {
                    log.incomingChannelDisabled(channel);
                }
            }

            for (Map.Entry<String, ConnectorConfig> entry : outgoings.entrySet()) {
                String channel = entry.getKey();
                ConnectorConfig config = entry.getValue();
                if (config.getOptionalValue(ConnectorConfig.CHANNEL_ENABLED_PROPERTY, Boolean.TYPE).orElse(true)) {
                    registry.register(channel, createSubscriber(channel, config),
                            config.getOptionalValue(ConnectorConfig.MERGE_PROPERTY, Boolean.class).orElse(false));
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

    private Publisher<? extends Message<?>> createPublisher(String name, Config config) {
        // Extract the type and throw an exception if missing
        String connector = getConnectorAttribute(config);

        InboundConnector inboundConnector = factories.getInboundConnectors().get(connector);
        if (inboundConnector == null) {
            throw ex.illegalArgumentUnknownConnector(name);
        }

        Multi<? extends Message<?>> publisher = Multi.createFrom().publisher(inboundConnector.getPublisher(config));

        for (PublisherDecorator decorator : publisherDecoratorInstance) {
            publisher = decorator.decorate(Multi.createFrom().publisher(publisher), name);
        }

        return publisher;
    }

    private Subscriber<? extends Message<?>> createSubscriber(String name, Config config) {
        // Extract the type and throw an exception if missing
        String connector = getConnectorAttribute(config);

        OutboundConnector outboundConnector = factories.getOutboundConnectors().get(connector);
        if (outboundConnector == null) {
            throw ex.illegalArgumentUnknownConnector(name);
        }

        return outboundConnector.getSubscriber(config);
    }
}
