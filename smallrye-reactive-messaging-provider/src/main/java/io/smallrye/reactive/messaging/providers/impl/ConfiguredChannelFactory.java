package io.smallrye.reactive.messaging.providers.impl;

import static io.smallrye.reactive.messaging.providers.helpers.CDIUtils.getSortedInstances;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;
import static io.smallrye.reactive.messaging.providers.impl.ConcurrencyConnectorConfig.getConcurrency;
import static io.smallrye.reactive.messaging.providers.impl.ConcurrencyConnectorConfig.isConcurrencyChannelName;
import static io.smallrye.reactive.messaging.providers.impl.ConcurrencyConnectorConfig.stripChannelNameOfSeparator;

import java.util.*;
import java.util.concurrent.Flow;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.spi.*;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelBinding;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.connector.ConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.DefaultEmitterConfiguration;
import io.smallrye.reactive.messaging.providers.connectors.WorkerPoolRegistry;
import io.smallrye.reactive.messaging.providers.extension.CompositePausableChannel;
import io.smallrye.reactive.messaging.providers.helpers.ConverterUtils;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;

/**
 * Look for stream factories and get instances.
 */
@ApplicationScoped
public class ConfiguredChannelFactory implements ChannelFactory, ChannelRegistar {

    public static final String SMALLRYE_PREFIX = "smallrye-";
    protected final Config config;
    protected final ChannelRegistry registry;
    private final ConnectorFactories factories;

    @Inject
    Instance<PublisherDecorator> publisherDecoratorInstance;

    @Inject
    @Any
    Instance<SubscriberDecorator> subscriberDecoratorInstance;

    @Inject
    @Any
    Instance<EmitterFactory<?>> emitterFactories;

    @Inject
    @Any
    Instance<MessageConverter> converters;

    @Inject
    WorkerPoolRegistry workerPoolRegistry;

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

    // --- Connector resolution ---

    private String getConnectorName(String name, Config config, Map<String, ?> connectors) {
        String connector = config.getValue("connector", String.class);

        if (connectors.containsKey(connector)) {
            return connector;
        } else if (!connector.startsWith(SMALLRYE_PREFIX) && connectors.containsKey(SMALLRYE_PREFIX + connector)) {
            return SMALLRYE_PREFIX + connector;
        } else {
            throw ex.illegalArgumentUnknownConnector(name);
        }
    }

    // --- Channel creation (internal, used by startup wiring and ConnectorConfiguration overloads) ---

    private Flow.Publisher<? extends Message<?>> createPublisher(String name, String connectorName, Config config) {
        InboundConnector inboundConnector = factories.getInboundConnectors().get(connectorName);
        if (inboundConnector == null) {
            throw ex.illegalArgumentUnknownConnector(name);
        }

        Multi<? extends Message<?>> publisher = MultiUtils.publisher(inboundConnector.getPublisher(config));

        // The connector owns the default of graceful-shutdown (e.g. Kafka declares true),
        // so resolve it once and expose the effective value to the decorators.
        boolean gracefulShutdown = inboundConnector.isGracefulShutdownEnabled(config);
        Config decoratorConfig = new OverrideConfig(config,
                Map.of(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, c -> gracefulShutdown));

        for (PublisherDecorator decorator : getSortedInstances(publisherDecoratorInstance)) {
            publisher = decorator.decorate(publisher, List.of(name), decoratorConfig);
        }

        return publisher;
    }

    private Flow.Subscriber<? extends Message<?>> createSubscriber(String name, String connectorName, Config config) {
        OutboundConnector outboundConnector = factories.getOutboundConnectors().get(connectorName);
        if (outboundConnector == null) {
            throw ex.illegalArgumentUnknownConnector(name);
        }

        return outboundConnector.getSubscriber(config);
    }

    Flow.Publisher<? extends Message<?>> incoming(String channel, Config config) {
        return incoming(channel, channel, ConnectorConfig.wrap(ConnectorFactory.INCOMING_PREFIX, channel, config));
    }

    Flow.Publisher<? extends Message<?>> incoming(String registrationChannel, String publisherChannel, Config config) {
        String connectorName = getConnectorName(publisherChannel, config, factories.getInboundConnectors());
        Flow.Publisher<? extends Message<?>> publisher = createPublisher(publisherChannel, connectorName, config);
        boolean broadcast = config.getOptionalValue(ConnectorConfig.BROADCAST_PROPERTY, Boolean.class).orElse(false);
        registry.register(registrationChannel, connectorName, publisher, broadcast);
        return publisher;
    }

    @SuppressWarnings("unchecked")
    <T> T outgoing(String channel, Config config, Class<T> emitterType) {
        config = ConnectorConfig.wrap(ConnectorFactory.OUTGOING_PREFIX, channel, config);
        EmitterFactoryFor emitterFactory = EmitterFactoryFor.Literal.of(emitterType);
        boolean broadcast = config.getOptionalValue(ConnectorConfig.BROADCAST_PROPERTY, Boolean.class)
                .orElse(false);
        int bufferSize = config.getOptionalValue("emitter.default-buffer-size", Integer.class).orElse(128);
        int defaultBufferSize = this.config != null
                ? this.config.getOptionalValue("mp.messaging.emitter.default-buffer-size", Integer.class).orElse(128)
                : 128;
        EmitterConfiguration emitterConfig = new DefaultEmitterConfiguration(channel, emitterFactory,
                OnOverflow.Strategy.BUFFER, bufferSize, broadcast, 0);
        EmitterFactory<?> factory = emitterFactories.select(emitterFactory).get();
        MessagePublisherProvider<?> emitter = factory.createEmitter(emitterConfig, defaultBufferSize);
        registry.register(channel, (Class) emitterType, emitter);

        Multi<? extends Message<?>> multi = Multi.createFrom().publisher(emitter.getPublisher());
        // Flag the config: an emitter channel is not a connector's incoming stream.
        Config decoratorConfig = Configs.emitterChannel(config);
        for (PublisherDecorator decorator : getSortedInstances(publisherDecoratorInstance)) {
            multi = decorator.decorate(multi, List.of(channel), decoratorConfig);
        }
        String connectorName = getConnectorName(channel, config, factories.getOutboundConnectors());
        // Register the upstream without a connector name: this is an outgoing connector channel,
        // so it must not be known as an incoming one (the lifecycle manager would drain it in phase 1).
        registry.register(channel, multi, false);
        Flow.Subscriber<? extends Message<?>> subscriber = createSubscriber(channel, connectorName, config);
        registry.register(channel, connectorName, subscriber, false);

        wireOutgoing(multi, subscriber, channel, config);

        return (T) emitter;
    }

    void outgoing(String channel, Config config, Flow.Publisher<? extends Message<?>> source) {
        config = ConnectorConfig.wrap(ConnectorFactory.OUTGOING_PREFIX, channel, config);
        Multi<? extends Message<?>> multi = MultiUtils.publisher(source);
        // Flag the config: this upstream feeds an outgoing channel, it is not a connector's incoming stream.
        Config decoratorConfig = Configs.emitterChannel(config);
        for (PublisherDecorator decorator : getSortedInstances(publisherDecoratorInstance)) {
            multi = decorator.decorate(multi, List.of(channel), decoratorConfig);
        }
        String connectorName = getConnectorName(channel, config, factories.getOutboundConnectors());
        // Register the upstream without a connector name: this is an outgoing connector channel,
        // so it must not be known as an incoming one (the lifecycle manager would drain it in phase 1).
        registry.register(channel, multi, false);
        Flow.Subscriber<? extends Message<?>> subscriber = createSubscriber(channel, connectorName, config);
        registry.register(channel, connectorName, subscriber, false);

        wireOutgoing(multi, subscriber, channel, config);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void wireOutgoing(Multi<? extends Message<?>> multi, Flow.Subscriber subscriber,
            String channel, Config config) {
        List<String> channelNames = List.of(channel);
        for (SubscriberDecorator decorator : getSortedInstances(subscriberDecoratorInstance)) {
            multi = decorator.decorate(multi, channelNames, config);
        }
        multi.subscribe().withSubscriber(subscriber);
    }

    Flow.Subscriber<? extends Message<?>> outgoing(String channel, Config config) {
        config = ConnectorConfig.wrap(ConnectorFactory.OUTGOING_PREFIX, channel, config);
        String connectorName = getConnectorName(channel, config, factories.getOutboundConnectors());
        Flow.Subscriber<? extends Message<?>> subscriber = createSubscriber(channel, connectorName, config);
        boolean merge = config.getOptionalValue(ConnectorConfig.MERGE_PROPERTY, Boolean.class).orElse(false);
        registry.register(channel, connectorName, subscriber, merge);
        return subscriber;
    }

    // --- ConnectorConfiguration overloads (public API) ---

    private ConnectorConfig wrapInConnectorConfig(String prefix, ConnectorConfiguration configuration) {
        String channel = configuration.getChannel();
        Config channelCfg = configuration.config();
        String channelPfx = ConnectorConfig.channelPrefix(prefix, channel);
        Map<String, Function<OverrideConfig, Object>> prefixed = new HashMap<>();
        for (String key : channelCfg.getPropertyNames()) {
            if (ConnectorFactory.CHANNEL_NAME_ATTRIBUTE.equalsIgnoreCase(key)) {
                continue;
            }
            channelCfg.getOptionalValue(key, String.class)
                    .ifPresent(v -> prefixed.put(channelPfx + key, oc -> v));
        }
        Config overall = this.config != null ? this.config : channelCfg;
        return ConnectorConfig.create(prefix, new OverrideConfig(overall, prefixed), channel);
    }

    /**
     * Channels created at runtime must have a name not already in use: close the existing channel first.
     */
    private void checkNotRegistered(String channel) {
        boolean registered;
        if (registry instanceof InternalChannelRegistry internal) {
            registered = internal.isRegistered(channel);
        } else {
            registered = !registry.getPublishers(channel).isEmpty() || !registry.getSubscribers(channel).isEmpty();
        }
        if (registered) {
            throw ex.channelAlreadyRegistered(channel);
        }
    }

    @Override
    public <T> ChannelBinding<T, T> incoming(IncomingConnectorConfiguration configuration, Class<T> payloadType) {
        checkNotRegistered(configuration.getChannel());
        ConnectorConfig cfg = wrapInConnectorConfig(ConnectorFactory.INCOMING_PREFIX, configuration);
        Flow.Publisher<? extends Message<?>> publisher = incoming(configuration.getChannel(),
                configuration.getChannel(), cfg);
        return bind(publisher, payloadType);
    }

    @Override
    public <T> T outgoing(OutgoingConnectorConfiguration configuration, Class<T> emitterType) {
        checkNotRegistered(configuration.getChannel());
        ConnectorConfig cfg = wrapInConnectorConfig(ConnectorFactory.OUTGOING_PREFIX, configuration);
        return outgoing(configuration.getChannel(), cfg, emitterType);
    }

    @Override
    public void outgoing(OutgoingConnectorConfiguration configuration,
            Flow.Publisher<? extends Message<?>> source) {
        checkNotRegistered(configuration.getChannel());
        ConnectorConfig cfg = wrapInConnectorConfig(ConnectorFactory.OUTGOING_PREFIX, configuration);
        outgoing(configuration.getChannel(), cfg, source);
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> outgoing(OutgoingConnectorConfiguration configuration) {
        checkNotRegistered(configuration.getChannel());
        ConnectorConfig cfg = wrapInConnectorConfig(ConnectorFactory.OUTGOING_PREFIX, configuration);
        return outgoing(configuration.getChannel(), cfg);
    }

    @Override
    public <T> ChannelBinding<T, T> bind(Flow.Publisher<? extends Message<?>> publisher, Class<T> payloadType) {
        Multi<? extends Message<?>> multi = MultiUtils.publisher(publisher);
        Multi<? extends Message<?>> converted = ConverterUtils.convert(multi, converters, payloadType);
        return new ChannelBindingImpl<>(converted, this, workerPoolRegistry);
    }

    @Override
    public ChannelBinding<?, ?> bind(Flow.Publisher<? extends Message<?>> publisher) {
        return new ChannelBindingImpl<>(MultiUtils.publisher(publisher), this, workerPoolRegistry);
    }

    // --- Startup ---

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
                // Create the channel only if the concurrency attribute is not present
                if (!isConcurrencyChannelName(name) || getConcurrency(stripChannelNameOfSeparator(name), root).isEmpty()) {
                    configs.put(name, new ConnectorConfig(prefix, root, name));
                }
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

    /**
     * A channel configured with {@code concurrency} is consumed through indexed channels, each with its own pausable
     * channel, while the connector knows the indexed names. Register a composite pausable under the base name, so the
     * logical channel can be paused and drained as a whole, and record the indexed names for the per-channel
     * lifecycle hooks.
     */
    private void registerConcurrentChannel(String channel, List<String> indexedChannels) {
        if (registry instanceof InternalChannelRegistry internal) {
            internal.registerIncomingConnectorChannels(channel, indexedChannels);
        }
        List<PausableChannel> parts = indexedChannels.stream()
                .map(registry::getPausable)
                .filter(Objects::nonNull)
                .toList();
        if (!parts.isEmpty() && registry.getPausable(channel) == null) {
            registry.register(channel, new CompositePausableChannel(parts));
        }
    }

    void register(Map<String, ConnectorConfig> incomings, Map<String, ConnectorConfig> outgoings) {
        try {
            for (Map.Entry<String, ConnectorConfig> entry : incomings.entrySet()) {
                String channel = entry.getKey();
                ConnectorConfig config = entry.getValue();
                if (config.getOptionalValue(ConnectorConfig.CHANNEL_ENABLED_PROPERTY, Boolean.TYPE).orElse(true)) {
                    int concurrency = getConcurrency(config).orElse(1);
                    if (concurrency <= 1) {
                        incoming(channel, config);
                    } else {
                        List<String> indexedChannels = new ArrayList<>();
                        for (int i = 0; i < concurrency; i++) {
                            ConcurrencyConnectorConfig indexedConfig = new ConcurrencyConnectorConfig(config, i + 1);
                            String indexedChannel = indexedConfig.getIndexedChannel();
                            incoming(channel, indexedChannel, indexedConfig);
                            indexedChannels.add(indexedChannel);
                        }
                        registerConcurrentChannel(channel, indexedChannels);
                    }
                } else {
                    log.incomingChannelDisabled(channel);
                }
            }

            for (Map.Entry<String, ConnectorConfig> entry : outgoings.entrySet()) {
                String channel = entry.getKey();
                ConnectorConfig config = entry.getValue();
                if (config.getOptionalValue(ConnectorConfig.CHANNEL_ENABLED_PROPERTY, Boolean.TYPE).orElse(true)) {
                    outgoing(channel, config);
                } else {
                    log.outgoingChannelDisabled(channel);
                }
            }
        } catch (RuntimeException e) { // NOSONAR
            log.unableToCreatePublisherOrSubscriber(e);
            throw e;
        }
    }

}
