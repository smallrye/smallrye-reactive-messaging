package io.smallrye.reactive.messaging.extension;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.*;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.annotations.OnOverflow;

/**
 * Class responsible for managing mediators
 */
@ApplicationScoped
public class MediatorManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediatorManager.class);
    public static final String STRICT_MODE_PROPERTY = "smallrye-messaging-strict-binding";
    private final boolean strictMode;

    private final CollectedMediatorMetadata collected = new CollectedMediatorMetadata();

    // TODO Populate this list
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    private final List<AbstractMediator> mediators = new ArrayList<>();

    @Inject
    @ConfigProperty(name = "smallrye.messaging.emitter.default-buffer-size", defaultValue = "127")
    long defaultBufferSize;

    @Inject
    @Any
    Instance<ChannelRegistar> streamRegistars;

    @Inject
    MediatorFactory mediatorFactory;

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    BeanManager beanManager;

    private boolean initialized;

    public MediatorManager() {
        strictMode = Boolean.parseBoolean(System.getProperty(STRICT_MODE_PROPERTY, "false"));
        if (strictMode) {
            LOGGER.debug("Strict mode enabled");
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public <T> void analyze(AnnotatedType<T> annotatedType, Bean<T> bean) {
        LOGGER.info("Scanning Type: {}", annotatedType.getJavaClass());
        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(method -> method.isAnnotationPresent(Incoming.class) || method.isAnnotationPresent(Outgoing.class))
                .forEach(method -> collected.add(method.getJavaMember(), bean));
    }

    @SuppressWarnings("unused")
    public <T> void analyze(Class<?> beanClass, Bean<T> bean) {
        Class<?> current = beanClass;
        while (current != Object.class) {
            Arrays.stream(current.getDeclaredMethods())
                    .filter(m -> m.isAnnotationPresent(Incoming.class) || m.isAnnotationPresent(Outgoing.class))
                    .forEach(m -> collected.add(m, bean));
            current = current.getSuperclass();
        }
    }

    @PreDestroy
    void shutdown() {
        LOGGER.info("Cancel subscriptions");
        subscriptions.forEach(Subscription::cancel);
        subscriptions.clear();
    }

    public void initializeAndRun() {
        if (initialized) {
            throw new IllegalStateException("MediatorManager was already initialized!");
        }
        LOGGER.info("Deployment done... start processing");

        streamRegistars.stream().forEach(ChannelRegistar::initialize);
        Set<String> unmanagedSubscribers = channelRegistry.getOutgoingNames();
        LOGGER.info("Initializing mediators");
        collected.mediators()
                .forEach(configuration -> {

                    AbstractMediator mediator = createMediator(configuration);

                    LOGGER.debug("Initializing {}", mediator.getMethodAsString());

                    try {
                        Object beanInstance = beanManager.getReference(configuration.getBean(), Object.class,
                                beanManager.createCreationalContext(configuration.getBean()));
                        mediator.initialize(beanInstance);
                    } catch (Throwable e) {
                        LOGGER.error("Unable to initialize mediator: " + mediator.getMethodAsString(), e);
                        return;
                    }

                    if (mediator.getConfiguration()
                            .shape() == Shape.PUBLISHER) {
                        LOGGER.debug("Registering {} as publisher {}", mediator.getConfiguration()
                                .methodAsString(),
                                mediator.getConfiguration()
                                        .getOutgoing());
                        channelRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
                    }
                    if (mediator.getConfiguration()
                            .shape() == Shape.SUBSCRIBER) {
                        LOGGER.debug("Registering {} as subscriber {}", mediator.getConfiguration()
                                .methodAsString(),
                                mediator.getConfiguration()
                                        .getIncoming());
                        channelRegistry.register(mediator.getConfiguration().getIncoming(), mediator.getComputedSubscriber());
                    }
                });

        try {
            weaving(unmanagedSubscribers);
        } catch (WeavingException e) {
            throw new DeploymentException(e);
        }
    }

    private void weaving(Set<String> unmanagedSubscribers) {
        // At that point all the publishers have been registered in the registry
        LOGGER.info("Connecting mediators");
        List<AbstractMediator> unsatisfied = getAllNonSatisfiedMediators();

        // This list contains the names of the streams that have bean connected and
        List<LazySource> lazy = new ArrayList<>();
        while (!unsatisfied.isEmpty()) {
            int numberOfUnsatisfiedBeforeLoop = unsatisfied.size();

            unsatisfied.forEach(mediator -> {
                LOGGER.info("Attempt to resolve {}", mediator.getMethodAsString());
                List<PublisherBuilder<? extends Message>> sources = channelRegistry
                        .getPublishers(mediator.configuration().getIncoming());
                Optional<PublisherBuilder<? extends Message>> maybeSource = getAggregatedSource(sources, mediator, lazy);
                maybeSource.ifPresent(publisher -> {
                    mediator.connectToUpstream(publisher);
                    LOGGER.info("Connecting {} to `{}` ({})", mediator.getMethodAsString(), mediator.configuration()
                            .getIncoming(), publisher);
                    if (mediator.configuration()
                            .getOutgoing() != null) {
                        channelRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
                    }
                });
            });

            unsatisfied = getAllNonSatisfiedMediators();
            int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

            if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
                // Stale!
                if (strictMode) {
                    throw new WeavingException("Impossible to bind mediators, some mediators are not connected: "
                            + unsatisfied.stream()
                                    .map(m -> m.configuration()
                                            .methodAsString())
                                    .collect(Collectors.toList())
                            + ", available publishers:" + channelRegistry.getIncomingNames() + ", "
                            + "available emitters: " + channelRegistry.getEmitterNames());
                } else {
                    LOGGER.warn("Impossible to bind mediators, some mediators are not connected: {}", unsatisfied.stream()
                            .map(m -> m.configuration()
                                    .methodAsString())
                            .collect(Collectors.toList()));
                    LOGGER.warn("Available publishers: {}", channelRegistry.getIncomingNames());
                    LOGGER.warn("Available emitters: {}", channelRegistry.getEmitterNames());
                }
                break;
            }
        }

        // Inject lazy sources
        lazy.forEach(l -> l.configure(channelRegistry, LOGGER));

        // Run
        mediators.stream()
                .filter(m -> m.configuration()
                        .shape() == Shape.SUBSCRIBER)
                .filter(AbstractMediator::isConnected)
                .forEach(AbstractMediator::run);

        // We also need to connect mediator and emitter to un-managed subscribers
        for (String name : unmanagedSubscribers) {
            List<AbstractMediator> list = lookupForMediatorsWithMatchingDownstream(name);
            EmitterImpl emitter = (EmitterImpl) channelRegistry.getEmitter(name);
            List<SubscriberBuilder<? extends Message, Void>> subscribers = channelRegistry.getSubscribers(name);
            for (AbstractMediator mediator : list) {
                if (subscribers.size() == 1) {
                    LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
                    mediator.getStream().to((SubscriberBuilder) subscribers.get(0)).run();
                } else if (subscribers.size() > 2) {
                    LOGGER.warn("{} subscribers consuming the stream {}", subscribers.size(), name);
                    subscribers.forEach(s -> {
                        LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
                        mediator.getStream().to((SubscriberBuilder) s).run();
                    });
                }
            }
            if (list.isEmpty() && emitter != null) {
                if (subscribers.size() == 1) {
                    LOGGER.info("Connecting emitter to sink {}", name);
                    ReactiveStreams.fromPublisher(emitter.getPublisher()).to(subscribers.get(0)).run();
                } else if (subscribers.size() > 2) {
                    LOGGER.warn("{} subscribers consuming the stream {}", subscribers.size(), name);
                    subscribers.forEach(s -> {
                        LOGGER.info("Connecting emitter to sink {}", name);
                        ReactiveStreams.fromPublisher(emitter.getPublisher()).to(s).run();
                    });
                }
            }
        }

        initialized = true;
    }

    private List<AbstractMediator> lookupForMediatorsWithMatchingDownstream(String name) {
        return mediators.stream()
                .filter(m -> m.configuration()
                        .getOutgoing() != null)
                .filter(m -> m.configuration()
                        .getOutgoing()
                        .equalsIgnoreCase(name))
                .collect(Collectors.toList());
    }

    private List<AbstractMediator> getAllNonSatisfiedMediators() {
        return mediators.stream()
                .filter(mediator -> !mediator.isConnected())
                .collect(Collectors.toList());
    }

    private AbstractMediator createMediator(MediatorConfiguration configuration) {
        AbstractMediator mediator = mediatorFactory.create(configuration);
        LOGGER.debug("Mediator created for {}", configuration.methodAsString());
        mediators.add(mediator);
        return mediator;
    }

    private Optional<PublisherBuilder<? extends Message>> getAggregatedSource(
            List<PublisherBuilder<? extends Message>> sources,
            AbstractMediator mediator,
            List<LazySource> lazy) {
        if (sources.isEmpty()) {
            return Optional.empty();
        }

        Merge.Mode merge = mediator.getConfiguration()
                .getMerge();
        if (merge != null) {
            LazySource lazySource = new LazySource(mediator.configuration()
                    .getIncoming(), merge);
            lazy.add(lazySource);
            return Optional.of(ReactiveStreams.fromPublisher(lazySource));
        }

        if (sources.size() > 1) {
            throw new WeavingException(mediator.configuration()
                    .getIncoming(), mediator.getMethodAsString(), sources.size());
        }
        return Optional.of(sources.get(0));

    }

    public void initializeEmitters(Map<String, OnOverflow> emitters) {
        for (Map.Entry<String, OnOverflow> e : emitters.entrySet()) {
            if (e.getValue() != null) {
                initializeEmitter(e.getKey(), e.getValue().value().name(), e.getValue().bufferSize(), defaultBufferSize);
            } else {
                initializeEmitter(e.getKey(), null, defaultBufferSize, defaultBufferSize);
            }
        }
    }

    public void initializeEmitter(String name, String overFlowStrategy, long bufferSize, long defaultBufferSize) {
        EmitterImpl<?> emitter = new EmitterImpl<>(name, overFlowStrategy, bufferSize, defaultBufferSize);
        Publisher<? extends Message<?>> publisher = emitter.getPublisher();
        channelRegistry.register(name, ReactiveStreams.fromPublisher(publisher));
        channelRegistry.register(name, emitter);
    }
}
