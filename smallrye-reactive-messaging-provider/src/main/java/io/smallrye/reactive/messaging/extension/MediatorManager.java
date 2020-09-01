package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;
import static io.smallrye.reactive.messaging.i18n.ProviderLogging.log;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.DeploymentException;
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

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.AbstractMediator;
import io.smallrye.reactive.messaging.ChannelRegistar;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.Invoker;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.WeavingException;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.assembly.AssemblyHook;
import io.smallrye.reactive.messaging.connectors.WorkerPoolRegistry;

/**
 * Class responsible for managing mediators
 */
@ApplicationScoped
public class MediatorManager {

    private static final int DEFAULT_BUFFER_SIZE = 128;

    public static final String STRICT_MODE_PROPERTY = "smallrye-messaging-strict-binding";
    private final boolean strictMode = Boolean.parseBoolean(System.getProperty(STRICT_MODE_PROPERTY, "false"));

    private final CollectedMediatorMetadata collected = new CollectedMediatorMetadata();

    // TODO Populate this list
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    private final List<AbstractMediator> mediators = new ArrayList<>();

    @Inject
    @ConfigProperty(name = "mp.messaging.emitter.default-buffer-size", defaultValue = "128")
    int defaultBufferSize;

    @Inject
    @ConfigProperty(name = "smallrye.messaging.emitter.default-buffer-size", defaultValue = "128")
    @Deprecated // Use mp.messaging.emitter.default-buffer-size instead
    int defaultBufferSizeLegacy;

    @Inject
    @Any
    Instance<ChannelRegistar> streamRegistars;

    @Inject
    MediatorFactory mediatorFactory;

    @Inject
    ChannelRegistry channelRegistry;

    @Inject
    BeanManager beanManager;

    @Inject
    WorkerPoolRegistry workerPoolRegistry;

    @Inject
    Instance<PublisherDecorator> decorators;

    @Inject
    Instance<AssemblyHook> hooks;

    @Inject
    HealthCenter health;

    private volatile boolean initialized;

    public MediatorManager() {
        if (strictMode) {
            log.strictModeEnabled();
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public <T> void analyze(AnnotatedType<T> annotatedType, Bean<T> bean) {
        log.scanningType(annotatedType.getJavaClass());
        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(this::hasMediatorAnnotations)
                .forEach(method -> collected.add(method.getJavaMember(), bean));
    }

    private <T> boolean hasMediatorAnnotations(AnnotatedMethod<? super T> method) {
        return method.isAnnotationPresent(Incomings.class) || method.isAnnotationPresent(Incoming.class)
                || method.isAnnotationPresent(Outgoing.class);
    }

    private boolean hasMediatorAnnotations(Method m) {
        return m.isAnnotationPresent(Incomings.class) || m.isAnnotationPresent(Incoming.class)
                || m.isAnnotationPresent(Outgoing.class);
    }

    @SuppressWarnings("unused")
    public <T> void analyze(Class<?> beanClass, Bean<T> bean) {
        Class<?> current = beanClass;
        while (current != Object.class) {
            Arrays.stream(current.getDeclaredMethods())
                    .filter(this::hasMediatorAnnotations)
                    .forEach(m -> collected.add(m, bean));

            current = current.getSuperclass();
        }
    }

    public void addAnalyzed(Collection<? extends MediatorConfiguration> mediators) {
        collected.addAll(mediators);
    }

    @PreDestroy
    void shutdown() {
        log.cancelSubscriptions();
        subscriptions.forEach(Subscription::cancel);
        subscriptions.clear();
    }

    public void initializeAndRun() {
        if (initialized) {
            throw ex.illegalStateForMediatorManagerAlreadyInitialized();
        }

        log.deploymentDoneStartProcessing();

        List<ChannelRegistar> registars = streamRegistars.stream().collect(Collectors.toList());
        for (ChannelRegistar registar : registars) {
            registar.initialize();
        }
        Set<String> unmanagedSubscribers = channelRegistry.getOutgoingNames();
        log.initializingMediators();

        // Before weaving hook
        Executor executor = Runnable::run;
        if (!hooks.isUnsatisfied()) {
            executor = hooks.get().before(registars, channelRegistry);
        }

        executor.execute(() -> {
            collected.mediators()
                    .forEach(configuration -> {

                        AbstractMediator mediator = createMediator(configuration);

                        log.initializingMethod(mediator.getMethodAsString());

                        mediator.setDecorators(decorators);
                        mediator.setHealth(health);
                        mediator.setWorkerPoolRegistry(workerPoolRegistry);

                        try {
                            Object beanInstance = beanManager.getReference(configuration.getBean(), Object.class,
                                    beanManager.createCreationalContext(configuration.getBean()));

                            if (configuration.getInvokerClass() != null) {
                                try {
                                    Constructor<? extends Invoker> constructorUsingBeanInstance = configuration
                                            .getInvokerClass()
                                            .getConstructor(Object.class);
                                    if (constructorUsingBeanInstance != null) {
                                        mediator.setInvoker(constructorUsingBeanInstance.newInstance(beanInstance));
                                    } else {
                                        mediator.setInvoker(configuration.getInvokerClass().getDeclaredConstructor()
                                                .newInstance());
                                    }

                                } catch (InstantiationException | IllegalAccessException e) {
                                    log.unableToCreateInvoker(configuration.getInvokerClass(), e);
                                    return;
                                }
                            }

                            mediator.initialize(beanInstance);
                        } catch (Throwable e) {
                            log.unableToInitializeMediator(mediator.getMethodAsString(), e);
                            return;
                        }

                        if (mediator.getConfiguration().shape() == Shape.PUBLISHER) {
                            log.registeringAsPublisher(mediator.getConfiguration().methodAsString(),
                                    mediator.getConfiguration().getOutgoing());
                            channelRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
                        }
                        if (mediator.getConfiguration().shape() == Shape.SUBSCRIBER) {
                            List<String> list = mediator.getConfiguration().getIncoming();
                            log.registeringAsSubscriber(mediator.getConfiguration().methodAsString(), list);
                            for (String l : list) {
                                channelRegistry.register(l, mediator.getComputedSubscriber());
                            }
                        }
                    });

            try {
                weaving(unmanagedSubscribers);
            } catch (WeavingException e) {
                throw new DeploymentException(e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void weaving(Set<String> unmanagedSubscribers) {
        // At that point all the publishers have been registered in the registry
        log.connectingMediators();
        List<AbstractMediator> unsatisfied = getAllNonSatisfiedMediators();
        // This list contains the names of the streams that have bean connected and
        List<LazySource> lazy = new ArrayList<>();
        while (!unsatisfied.isEmpty()) {
            int numberOfUnsatisfiedBeforeLoop = unsatisfied.size();

            unsatisfied.forEach(mediator -> {
                log.attemptToResolve(mediator.getMethodAsString());
                List<String> list = mediator.configuration().getIncoming();
                if (list.size() == 1) {
                    // Single source.
                    List<PublisherBuilder<? extends Message<?>>> sources = channelRegistry.getPublishers(list.get(0));
                    Optional<PublisherBuilder<? extends Message<?>>> maybeSource = getAggregatedSource(sources, list.get(0),
                            mediator, lazy);
                    maybeSource.ifPresent(publisher -> {
                        mediator.connectToUpstream(publisher);
                        log.connectingTo(mediator.getMethodAsString(), list, publisher);
                        if (mediator.configuration().getOutgoing() != null) {
                            channelRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
                        }
                    });
                } else {
                    List<PublisherBuilder<? extends Message<?>>> upstreams = new ArrayList<>();
                    for (String sn : list) {
                        List<PublisherBuilder<? extends Message<?>>> sources = channelRegistry.getPublishers(sn);
                        Optional<PublisherBuilder<? extends Message<?>>> maybeSource = getAggregatedSource(sources, sn,
                                mediator,
                                lazy);
                        maybeSource.ifPresent(upstreams::add);
                    }

                    if (upstreams.size() == list.size()) {
                        // We have all our upstreams
                        Multi<? extends Message<?>> merged = Multi.createBy().merging()
                                .streams(upstreams.stream().map(PublisherBuilder::buildRs).collect(Collectors.toList()));
                        mediator.connectToUpstream(ReactiveStreams.fromPublisher(merged));
                        log.connectingTo(mediator.getMethodAsString(), list);
                        if (mediator.configuration().getOutgoing() != null) {
                            channelRegistry.register(mediator.getConfiguration().getOutgoing(), mediator.getStream());
                        }
                    }
                }
            });

            unsatisfied = getAllNonSatisfiedMediators();
            int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

            if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
                // Stale!
                if (strictMode) {
                    throw ex.weavingImposibleToBind(unsatisfied.stream()
                            .map(m -> m.configuration().methodAsString())
                            .collect(Collectors.toList()),
                            channelRegistry.getIncomingNames(),
                            channelRegistry.getEmitterNames());
                } else {
                    log.impossibleToBindMediators(
                            unsatisfied.stream().map(m -> m.configuration().methodAsString()).collect(Collectors.toList()),
                            channelRegistry.getIncomingNames(),
                            channelRegistry.getEmitterNames());
                }
                break;
            }
        }

        // Inject lazy sources
        lazy.forEach(l -> l.configure(channelRegistry));

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
            List<SubscriberBuilder<? extends Message<?>, Void>> subscribers = channelRegistry.getSubscribers(name);
            for (AbstractMediator mediator : list) {
                if (subscribers.size() == 1) {
                    log.connectingMethodToSink(mediator.getMethodAsString(), name);
                    mediator.getStream().to((SubscriberBuilder<Message<?>, Void>) subscribers.get(0)).run();
                } else if (subscribers.size() > 2) {
                    log.numberOfSubscribersConsumingStream(subscribers.size(), name);
                    subscribers.forEach(s -> {
                        log.connectingMethodToSink(mediator.getMethodAsString(), name);
                        mediator.getStream().to((SubscriberBuilder<Message<?>, Void>) s).run();
                    });
                }
            }

            if (list.isEmpty() && emitter != null) {
                if (subscribers.size() == 1) {
                    log.connectingEmitterToSink(name);
                    ReactiveStreams.fromPublisher(emitter.getPublisher()).to(subscribers.get(0)).run();
                } else if (subscribers.size() > 2) {
                    log.numberOfSubscribersConsumingStream(subscribers.size(), name);
                    subscribers.forEach(s -> {
                        log.connectingEmitterToSink(name);
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
        log.mediatorCreated(configuration.methodAsString());
        mediators.add(mediator);
        return mediator;
    }

    private Optional<PublisherBuilder<? extends Message<?>>> getAggregatedSource(
            List<PublisherBuilder<? extends Message<?>>> sources,
            String sourceName,
            AbstractMediator mediator,
            List<LazySource> lazy) {
        if (sources.isEmpty()) {
            return Optional.empty();
        }

        Merge.Mode merge = mediator.getConfiguration()
                .getMerge();
        if (merge != null) {
            LazySource lazySource = new LazySource(sourceName, merge);
            lazy.add(lazySource);
            return Optional.of(ReactiveStreams.fromPublisher(lazySource));
        }

        if (sources.size() > 1) {
            throw new WeavingException(sourceName, mediator.getMethodAsString(), sources.size());
        }
        return Optional.of(sources.get(0));

    }

    public void initializeEmitters(List<EmitterConfiguration> emitters) {
        for (EmitterConfiguration config : emitters) {
            int bufferSize = getDefaultBufferSize();
            initializeEmitter(config, bufferSize);
        }
    }

    private int getDefaultBufferSize() {
        if (defaultBufferSize == DEFAULT_BUFFER_SIZE && defaultBufferSizeLegacy != DEFAULT_BUFFER_SIZE) {
            return defaultBufferSizeLegacy;
        } else {
            return defaultBufferSize;
        }
    }

    public void initializeEmitter(EmitterConfiguration emitterConfiguration, long defaultBufferSize) {
        EmitterImpl<?> emitter = new EmitterImpl<>(emitterConfiguration, defaultBufferSize);
        Publisher<? extends Message<?>> publisher = emitter.getPublisher();
        channelRegistry.register(emitterConfiguration.name, ReactiveStreams.fromPublisher(publisher));
        channelRegistry.register(emitterConfiguration.name, emitter);
    }
}
