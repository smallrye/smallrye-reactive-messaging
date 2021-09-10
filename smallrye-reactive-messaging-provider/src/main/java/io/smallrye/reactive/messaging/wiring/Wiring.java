package io.smallrye.reactive.messaging.wiring;

import java.util.*;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.*;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.AbstractMediator;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.extension.*;
import io.smallrye.reactive.messaging.i18n.ProviderLogging;
import io.smallrye.reactive.messaging.impl.ConnectorConfig;

@ApplicationScoped
public class Wiring {

    public static final int DEFAULT_BUFFER_SIZE = 128;

    /**
     * Property enabling / disabling the ability to automatically wire unresolved upstream and downstream to a connector.
     * Even when enabled, it only uses the connector when there is only one connector available (so no ambiguity).
     */
    @Inject
    @ConfigProperty(name = "smallrye.messaging.wiring.auto-wire-to-connector", defaultValue = "false")
    boolean autoWireToSingleConnector;

    @Inject
    @ConfigProperty(name = "mp.messaging.emitter.default-buffer-size", defaultValue = "128")
    int defaultBufferSize;

    @Inject
    @ConfigProperty(name = "smallrye.messaging.emitter.default-buffer-size", defaultValue = "128")
    @Deprecated // Use mp.messaging.emitter.default-buffer-size instead
    int defaultBufferSizeLegacy;

    @Inject
    MediatorManager manager;

    @Inject
    BeanManager beanManager;

    @Inject
    @Any
    Instance<IncomingConnectorFactory> incomings;

    @Inject
    @Any
    Instance<OutgoingConnectorFactory> outgoings;

    private final List<Component> components;

    private Graph graph;

    private boolean strictMode;
    private ChannelRegistry registry;

    public Wiring() {
        components = new ArrayList<>();
    }

    public void prepare(boolean strictMode, ChannelRegistry registry, List<EmitterConfiguration> emitters,
            List<ChannelConfiguration> channels,
            List<MediatorConfiguration> mediators) {
        this.strictMode = strictMode;
        this.registry = registry;

        for (MediatorConfiguration mediator : mediators) {
            if (mediator.getOutgoing() != null && !mediator.getIncoming().isEmpty()) {
                components.add(new ProcessorMediatorComponent(manager, mediator));
            } else if (mediator.getOutgoing() != null) {
                components.add(new PublisherMediatorComponent(manager, mediator));
            } else {
                components.add(new SubscriberMediatorComponent(manager, mediator));
            }
        }

        for (ChannelConfiguration channel : channels) {
            components.add(new InjectedChannelComponent(channel, strictMode));
        }

        for (EmitterConfiguration emitter : emitters) {
            components.add(new EmitterComponent(emitter, defaultBufferSize, defaultBufferSizeLegacy));
        }

        // At that point, the registry only contains connectors or managed channels
        for (Map.Entry<String, Boolean> entry : registry.getIncomingChannels().entrySet()) {
            components.add(new InboundConnectorComponent(entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Boolean> entry : registry.getOutgoingChannels().entrySet()) {
            components.add(new OutgoingConnectorComponent(entry.getKey(), entry.getValue()));
        }
    }

    public Graph resolve() {
        ProviderLogging.log.startGraphResolution(components.size());
        long begin = System.nanoTime();
        Set<Component> resolved = new LinkedHashSet<>();
        Set<ConsumingComponent> unresolved = new LinkedHashSet<>();

        // Initialize lists
        for (Component component : components) {
            if (component.isUpstreamResolved()) {
                resolved.add(component);
            } else {
                unresolved.add((ConsumingComponent) component);
            }
        }

        boolean doneOrStale = false;
        // Until everything is resolved or we got staled
        while (!doneOrStale) {
            List<ConsumingComponent> resolvedDuringThisTurn = new ArrayList<>();
            for (ConsumingComponent component : unresolved) {
                List<String> incomings = component.incomings();
                for (String incoming : incomings) {
                    List<Component> matches = getMatchesFor(incoming, resolved);
                    if (!matches.isEmpty()) {
                        matches.forEach(m -> bind(component, m));
                        if (component.isUpstreamResolved()) {
                            resolvedDuringThisTurn.add(component);
                        }
                    }
                }
            }

            resolved.addAll(resolvedDuringThisTurn);
            resolvedDuringThisTurn.forEach(unresolved::remove);

            doneOrStale = resolvedDuringThisTurn.isEmpty() || unresolved.isEmpty();

            // Update components consuming multiple incomings.
            for (Component component : resolved) {
                if (component instanceof ConsumingComponent) {
                    ConsumingComponent cc = (ConsumingComponent) component;
                    List<String> incomings = cc.incomings();
                    for (String incoming : incomings) {
                        List<Component> matches = getMatchesFor(incoming, resolved);
                        for (Component match : matches) {
                            bind(cc, match);
                        }
                    }
                }
            }
        }

        // Attempt to resolve from the unresolved set.
        List<ConsumingComponent> newlyResolved = new ArrayList<>();
        for (ConsumingComponent c : unresolved) {
            for (String incoming : c.incomings()) {
                // searched in unresolved
                List<Component> matches = getMatchesFor(incoming, unresolved);
                if (!matches.isEmpty()) {
                    newlyResolved.add(c);
                    matches.forEach(m -> bind(c, m));
                }
            }
        }

        if (!newlyResolved.isEmpty()) {
            newlyResolved.forEach(unresolved::remove);
            resolved.addAll(newlyResolved);
        }

        if (autoWireToSingleConnector) {
            handleSingleConnectorSituation(resolved, unresolved);
        }

        graph = new Graph(strictMode, resolved, unresolved);
        long duration = System.nanoTime() - begin;
        ProviderLogging.log.completedGraphResolution(duration);
        return graph;

    }

    /**
     * In the case there is a single connector, bind all unresolved components to this connector.
     * For missing outgoing, bind all pending outgoing to the outgoing connector.
     *
     * @param resolved the list of resolved component
     * @param unresolved the list of unresolved components, empty if there is a single incoming connector as we connect
     *        all components to this one.
     */
    private void handleSingleConnectorSituation(Set<Component> resolved, Set<ConsumingComponent> unresolved) {
        ProviderLogging.log.connectorAutoWiringEnabled();
        // if we are in a single "connector" situation, try to see if we can use them
        // we do it in two steps: first incoming, then outgoing
        //
        String singleIncomingConnector = getSingleConnector(IncomingConnectorFactory.class);
        String singleOutgoingConnector = getSingleConnector(OutgoingConnectorFactory.class);

        if (singleIncomingConnector != null) {
            for (ConsumingComponent c : unresolved) {
                // For each unresolved channel, we use the connector
                // For multi-incomings, we only resolve the first one.
                // To satisfy the graph, we need to register the connector in the registry, bind the component and the
                // connector and add the component to the resolved list.
                String channel = c.incomings().get(0);
                InboundConnectorComponent icc = new InboundConnectorComponent(channel, false);

                IncomingConnectorFactory connector = incomings
                        .select(IncomingConnectorFactory.class, ConnectorLiteral.of(singleIncomingConnector)).get();
                ConnectorConfig config = new ConnectorConfig(ConnectorFactory.INCOMING_PREFIX, ConfigProvider.getConfig(),
                        channel, singleIncomingConnector);
                registry.register(channel, connector.getPublisherBuilder(config));
                ProviderLogging.log.connectorAutoWiringIncomingChannel(channel, c.toString(), singleIncomingConnector);
                bind(c, icc);
                resolved.add(c);
                resolved.add(icc);
                components.add(icc);
            }

            unresolved.clear();
        }
        if (singleOutgoingConnector != null) {
            List<Component> toBeAddedToResolved = new ArrayList<>();
            for (Component c : components) {
                if (c.outgoing().isPresent() && !c.isDownstreamResolved()) {
                    String channel = c.outgoing().orElseThrow(() -> new NoSuchElementException("No outgoing channel for " + c));
                    OutgoingConnectorComponent occ = new OutgoingConnectorComponent(channel, false);
                    OutgoingConnectorFactory connector = outgoings
                            .select(OutgoingConnectorFactory.class, ConnectorLiteral.of(singleOutgoingConnector)).get();
                    ConnectorConfig config = new ConnectorConfig(ConnectorFactory.OUTGOING_PREFIX, ConfigProvider.getConfig(),
                            channel, singleOutgoingConnector);
                    registry.register(channel, connector.getSubscriberBuilder(config));
                    ProviderLogging.log.connectorAutoWiringOutgoingChannel(channel, c.toString(), singleIncomingConnector);
                    bind(occ, c);
                    resolved.add(c);
                    toBeAddedToResolved.add(occ);
                }
            }
            resolved.addAll(toBeAddedToResolved);
            components.addAll(toBeAddedToResolved);
        }
    }

    private String getSingleConnector(Class<?> clazz) {
        List<String> factories = beanManager.getBeans(clazz, Any.Literal.INSTANCE).stream()
                .flatMap(b -> b.getQualifiers().stream())
                .filter(a -> a instanceof Connector)
                .map(a -> ((Connector) a).value())
                .collect(Collectors.toList());
        if (factories.size() == 1) {
            return factories.get(0);
        }
        ProviderLogging.log.disablingConnectorAutoWiring(factories.size(), clazz.getName());
        return null;
    }

    public Graph getGraph() {
        return graph;
    }

    private void bind(ConsumingComponent consumer, Component provider) {
        consumer.connectUpstream(provider);
        provider.connectDownstream(consumer);
    }

    private List<Component> getMatchesFor(String incoming, Set<? extends Component> candidates) {
        List<Component> matches = new ArrayList<>();
        for (Component component : candidates) {
            Optional<String> outgoing = component.outgoing();
            if (outgoing.isPresent() && outgoing.get().equalsIgnoreCase(incoming)) {
                matches.add(component);
            }
        }
        return matches;
    }

    public interface Component {

        void validate() throws WiringException;

        boolean isUpstreamResolved();

        boolean isDownstreamResolved();

        default Optional<String> outgoing() {
            return Optional.empty();
        }

        default List<String> incomings() {
            return Collections.emptyList();
        }

        default Set<Component> downstreams() {
            return Collections.emptySet();
        }

        default Set<Component> upstreams() {
            return Collections.emptySet();
        }

        default void connectDownstream(Component downstream) {
            throw new UnsupportedOperationException("Downstream connection not expected for " + this);
        }

        void materialize(ChannelRegistry registry);
    }

    interface PublishingComponent extends Component {
        boolean broadcast();

        int getRequiredNumberOfSubscribers();

        default String getOutgoingChannel() {
            return outgoing().orElseThrow(() -> new IllegalStateException("Outgoing not configured for " + this));
        }

        @Override
        default boolean isDownstreamResolved() {
            return !downstreams().isEmpty();
        }

        @Override
        default void connectDownstream(Component downstream) {
            downstreams().add(downstream);
        }
    }

    interface ConsumingComponent extends Component {

        @Override
        default boolean isUpstreamResolved() {
            return !upstreams().isEmpty();
        }

        default void connectUpstream(Component upstream) {
            upstreams().add(upstream);
        }

        boolean merge();
    }

    interface NoUpstreamComponent extends Component {
        @Override
        default boolean isUpstreamResolved() {
            return true;
        }
    }

    interface NoDownstreamComponent extends Component {
        @Override
        default boolean isDownstreamResolved() {
            return true;
        }
    }

    static class InboundConnectorComponent implements PublishingComponent, NoUpstreamComponent {

        private final String name;
        private final boolean broadcast;
        private final Set<Component> downstreams = new LinkedHashSet<>();

        public InboundConnectorComponent(String name, boolean broadcast) {
            this.name = name;
            this.broadcast = broadcast;
        }

        @Override
        public Optional<String> outgoing() {
            return Optional.of(name);
        }

        @Override
        public Set<Component> downstreams() {
            return downstreams;
        }

        @Override
        public void materialize(ChannelRegistry registry) {
            // We are already registered and created.
        }

        @Override
        public boolean broadcast() {
            return broadcast;
        }

        @Override
        public int getRequiredNumberOfSubscribers() {
            return 0;
        }

        @Override
        public String toString() {
            return "IncomingConnector{channel:'" + name + "', attribute:'mp.messaging.incoming." + name + "'}";
        }

        @Override
        public void validate() throws WiringException {
            if (!broadcast && downstreams().size() > 1) {
                throw new TooManyDownstreamCandidatesException(this);
            }
        }
    }

    static class OutgoingConnectorComponent implements ConsumingComponent, NoDownstreamComponent {

        private final String name;
        private final Set<Component> upstreams = new LinkedHashSet<>();
        private final boolean merge;

        public OutgoingConnectorComponent(String name, boolean merge) {
            this.name = name;
            this.merge = merge;
        }

        @Override
        public List<String> incomings() {
            return Collections.singletonList(name);
        }

        @Override
        public boolean merge() {
            return merge;
        }

        @Override
        public void connectUpstream(Component upstream) {
            upstreams.add(upstream);
        }

        @Override
        public Set<Component> upstreams() {
            return upstreams;
        }

        @Override
        public String toString() {
            return "OutgoingConnector{channel:'" + name + "', attribute:'mp.messaging.outgoing." + name + "'}";
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public void materialize(ChannelRegistry registry) {
            List<PublisherBuilder<? extends Message<?>>> publishers = registry.getPublishers(name);
            Multi<? extends Message<?>> merged;
            if (publishers.size() == 1) {
                merged = Multi.createFrom().publisher(publishers.get(0).buildRs());
            } else {
                merged = Multi.createBy().merging()
                        .streams(publishers.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList()));
            }
            SubscriberBuilder<? extends Message<?>, Void> connector = registry.getSubscribers(name).get(0);
            Subscriber subscriber = connector.build();
            merged.subscribe().withSubscriber(subscriber);
        }

        @Override
        public void validate() throws WiringException {
            if (upstreams().size() > 1 && !merge) {
                throw new TooManyUpstreamCandidatesException(this);
            }
        }
    }

    static class InjectedChannelComponent implements ConsumingComponent, NoDownstreamComponent {

        private final String name;
        private final Set<Component> upstreams = new LinkedHashSet<>();
        private final boolean strict;

        public InjectedChannelComponent(ChannelConfiguration configuration, boolean strict) {
            this.name = configuration.channelName;
            this.strict = strict;
        }

        @Override
        public List<String> incomings() {
            return Collections.singletonList(name);
        }

        @Override
        public boolean merge() {
            return !strict;
        }

        @Override
        public Set<Component> upstreams() {
            return upstreams;
        }

        @Override
        public String toString() {
            return "@Channel{channel:'" + name + "'}";
        }

        @Override
        public void materialize(ChannelRegistry registry) {
            // Nothing to be done for channel - look up happen during the subscription.
        }

        @Override
        public void validate() throws WiringException {
            if (strict && upstreams().size() > 1) {
                throw new TooManyUpstreamCandidatesException(this);
            }
        }
    }

    static class EmitterComponent implements PublishingComponent, NoUpstreamComponent {

        private final EmitterConfiguration configuration;
        private final Set<Component> downstreams = new LinkedHashSet<>();
        private final int defaultBufferSize;
        private final int defaultBufferSizeLegacy;

        public EmitterComponent(EmitterConfiguration configuration, int defaultBufferSize, int defaultBufferSizeLegacy) {
            this.configuration = configuration;
            this.defaultBufferSize = defaultBufferSize;
            this.defaultBufferSizeLegacy = defaultBufferSizeLegacy;
        }

        @Override
        public Optional<String> outgoing() {
            return Optional.of(configuration.name);
        }

        @Override
        public Set<Component> downstreams() {
            return downstreams;
        }

        @Override
        public boolean broadcast() {
            return configuration.broadcast;
        }

        @Override
        public int getRequiredNumberOfSubscribers() {
            return configuration.numberOfSubscriberBeforeConnecting;
        }

        @Override
        public String toString() {
            return "Emitter{channel:'" + getOutgoingChannel() + "'}";
        }

        @Override
        public void materialize(ChannelRegistry registry) {
            Publisher<? extends Message<?>> publisher;
            int def = getDefaultBufferSize();
            if (configuration.isMutinyEmitter) {
                MutinyEmitterImpl<?> mutinyEmitter = new MutinyEmitterImpl<>(configuration, def);
                publisher = mutinyEmitter.getPublisher();
                registry.register(configuration.name, mutinyEmitter);
            } else {
                EmitterImpl<?> emitter = new EmitterImpl<>(configuration, def);
                publisher = emitter.getPublisher();
                registry.register(configuration.name, emitter);
            }
            registry.register(configuration.name, ReactiveStreams.fromPublisher(publisher));
        }

        private int getDefaultBufferSize() {
            if (defaultBufferSize == DEFAULT_BUFFER_SIZE && defaultBufferSizeLegacy != DEFAULT_BUFFER_SIZE) {
                return defaultBufferSizeLegacy;
            } else {
                return defaultBufferSize;
            }
        }

        @Override
        public void validate() throws WiringException {
            if (!configuration.broadcast && downstreams().size() > 1) {
                throw new TooManyDownstreamCandidatesException(this);
            }

            if (broadcast()
                    && getRequiredNumberOfSubscribers() != 0 && getRequiredNumberOfSubscribers() != downstreams.size()) {
                throw new UnsatisfiedBroadcastException(this);
            }
        }
    }

    abstract static class MediatorComponent implements Component {
        final MediatorConfiguration configuration;
        final MediatorManager manager;

        protected MediatorComponent(MediatorManager manager, MediatorConfiguration configuration) {
            this.configuration = configuration;
            this.manager = manager;
        }
    }

    static class PublisherMediatorComponent extends MediatorComponent implements PublishingComponent, NoUpstreamComponent {

        private final Set<Component> downstreams = new LinkedHashSet<>();

        protected PublisherMediatorComponent(MediatorManager manager, MediatorConfiguration configuration) {
            super(manager, configuration);
        }

        @Override
        public Optional<String> outgoing() {
            return Optional.of(configuration.getOutgoing());
        }

        @Override
        public Set<Component> downstreams() {
            return downstreams;
        }

        @Override
        public void materialize(ChannelRegistry registry) {
            AbstractMediator mediator = manager.createMediator(configuration);
            registry.register(configuration.getOutgoing(), mediator.getStream());
        }

        @Override
        public boolean broadcast() {
            return configuration.getBroadcast();
        }

        @Override
        public int getRequiredNumberOfSubscribers() {
            return configuration.getNumberOfSubscriberBeforeConnecting();
        }

        @Override
        public String toString() {
            return "PublisherMethod{" +
                    "method:'" + configuration.methodAsString() + "', outgoing:'" + getOutgoingChannel() + "'}";
        }

        @Override
        public void validate() throws WiringException {
            if (!broadcast() && downstreams().size() > 1) {
                throw new TooManyDownstreamCandidatesException(this);
            }
            if (broadcast()
                    && getRequiredNumberOfSubscribers() != 0 && getRequiredNumberOfSubscribers() != downstreams.size()) {
                throw new UnsatisfiedBroadcastException(this);
            }
        }
    }

    static class SubscriberMediatorComponent extends MediatorComponent implements ConsumingComponent, NoDownstreamComponent {

        private final Set<Component> upstreams = new LinkedHashSet<>();

        protected SubscriberMediatorComponent(MediatorManager manager, MediatorConfiguration configuration) {
            super(manager, configuration);
        }

        @Override
        public Set<Component> upstreams() {
            return upstreams;
        }

        @Override
        public List<String> incomings() {
            return configuration.getIncoming();
        }

        @Override
        public boolean merge() {
            return configuration.getMerge() != null;
        }

        @Override
        public void materialize(ChannelRegistry registry) {
            AbstractMediator mediator = manager.createMediator(configuration);

            boolean concat = configuration.getMerge() == Merge.Mode.CONCAT;
            boolean one = configuration.getMerge() == Merge.Mode.ONE;

            Multi<? extends Message<?>> aggregates;
            List<PublisherBuilder<? extends Message<?>>> publishers = new ArrayList<>();
            for (String channel : configuration.getIncoming()) {
                publishers.addAll(registry.getPublishers(channel));
            }

            if (publishers.size() == 1) {
                aggregates = Multi.createFrom().publisher(publishers.get(0).buildRs());
            } else if (concat) {
                aggregates = Multi.createBy().concatenating()
                        .streams(publishers.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList()));
            } else if (one) {
                aggregates = Multi.createFrom().publisher(publishers.get(0).buildRs());
            } else {
                aggregates = Multi.createBy().merging()
                        .streams(publishers.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList()));
            }

            mediator.connectToUpstream(ReactiveStreams.fromPublisher(aggregates));

            SubscriberBuilder<Message<?>, Void> subscriber = mediator.getComputedSubscriber();
            incomings().forEach(s -> registry.register(s, subscriber));

            mediator.run();
        }

        @Override
        public String toString() {
            return "SubscriberMethod{" +
                    "method:'" + configuration.methodAsString() + "', incoming:'" + String
                            .join(",", configuration.getIncoming())
                    + "'}";
        }

        private boolean hasAllUpstreams() {
            // A subscriber can have multiple incomings - all of them must be bound.
            for (String incoming : incomings()) {
                // For each incoming, check that we have a match
                if (upstreams().stream().noneMatch(c -> incoming.equals(c.outgoing().orElse(null)))) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public boolean isUpstreamResolved() {
            return hasAllUpstreams();
        }

        @Override
        public void validate() throws WiringException {
            // Check that for each incoming we have a single upstream or a merge strategy
            for (String incoming : incomings()) {
                List<Component> components = downstreams().stream()
                        .filter(c -> incoming.equals(c.outgoing().orElse(null)))
                        .collect(Collectors.toList());
                if (components.size() > 1 && !merge()) {
                    throw new TooManyUpstreamCandidatesException(this, incoming, components);
                }
            }

            if (!merge() && upstreams.size() != incomings().size()) {
                throw new TooManyUpstreamCandidatesException(this);
            }
        }
    }

    static class ProcessorMediatorComponent extends MediatorComponent
            implements ConsumingComponent, PublishingComponent {

        private final Set<Component> upstreams = new LinkedHashSet<>();
        private final Set<Component> downstreams = new LinkedHashSet<>();

        protected ProcessorMediatorComponent(MediatorManager manager, MediatorConfiguration configuration) {
            super(manager, configuration);
        }

        @Override
        public Set<Component> upstreams() {
            return upstreams;
        }

        @Override
        public List<String> incomings() {
            return configuration.getIncoming();
        }

        @Override
        public boolean merge() {
            return configuration.getMerge() != null;
        }

        @Override
        public Optional<String> outgoing() {
            return Optional.of(configuration.getOutgoing());
        }

        @Override
        public Set<Component> downstreams() {
            return downstreams;
        }

        @Override
        public boolean broadcast() {
            return configuration.getBroadcast();
        }

        @Override
        public int getRequiredNumberOfSubscribers() {
            return configuration.getNumberOfSubscriberBeforeConnecting();
        }

        private boolean hasAllUpstreams() {
            // A subscriber can have multiple incomings - all of them must be bound.
            for (String incoming : incomings()) {
                // For each incoming, check that we have a match
                if (upstreams().stream().noneMatch(c -> incoming.equals(c.outgoing().orElse(null)))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean isUpstreamResolved() {
            return hasAllUpstreams();
        }

        @Override
        public String toString() {
            return "ProcessingMethod{" +
                    "method:'" + configuration.methodAsString()
                    + "', incoming:'" + String.join(",", configuration.getIncoming())
                    + "', outgoing:'" + getOutgoingChannel() + "'}";
        }

        @Override
        public void materialize(ChannelRegistry registry) {
            AbstractMediator mediator = manager.createMediator(configuration);

            boolean concat = configuration.getMerge() == Merge.Mode.CONCAT;
            boolean one = configuration.getMerge() == Merge.Mode.ONE;

            Multi<? extends Message<?>> aggregates;
            List<PublisherBuilder<? extends Message<?>>> publishers = new ArrayList<>();
            for (String channel : configuration.getIncoming()) {
                publishers.addAll(registry.getPublishers(channel));
            }
            if (publishers.size() == 1) {
                aggregates = Multi.createFrom().publisher(publishers.get(0).buildRs());
            } else if (concat) {
                aggregates = Multi.createBy().concatenating()
                        .streams(publishers.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList()));
            } else if (one) {
                aggregates = Multi.createFrom().publisher(publishers.get(0).buildRs());
            } else {
                aggregates = Multi.createBy().merging()
                        .streams(publishers.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList()));
            }

            mediator.connectToUpstream(ReactiveStreams.fromPublisher(aggregates));

            registry.register(getOutgoingChannel(), mediator.getStream());
        }

        @Override
        public void validate() throws WiringException {
            // Check that for each incoming we have a single upstream or a merge strategy
            for (String incoming : incomings()) {
                List<Component> components = downstreams().stream()
                        .filter(c -> incoming.equals(c.outgoing().orElse(null)))
                        .collect(Collectors.toList());
                if (components.size() > 1 && !merge()) {
                    throw new TooManyUpstreamCandidatesException(this, incoming, components);
                }
            }

            if (!merge() && upstreams.size() != incomings().size()) {
                throw new TooManyUpstreamCandidatesException(this);
            }

            if (!broadcast() && downstreams().size() > 1) {
                throw new TooManyDownstreamCandidatesException(this);
            }

            if (broadcast()
                    && getRequiredNumberOfSubscribers() != 0 && getRequiredNumberOfSubscribers() != downstreams.size()) {
                throw new UnsatisfiedBroadcastException(this);
            }
        }
    }

}
