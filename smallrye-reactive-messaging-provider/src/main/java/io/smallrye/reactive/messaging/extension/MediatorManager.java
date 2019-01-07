package io.smallrye.reactive.messaging.extension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.reactive.messaging.AbstractMediator;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.WeavingException;
import io.smallrye.reactive.messaging.annotations.Merge;

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
    @Any
    Instance<StreamRegistar> streamRegistars;

    @Inject
    MediatorFactory mediatorFactory;

    @Inject
    StreamRegistry streamRegistry;

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
        LOGGER.info("Scanning Type: " + annotatedType.getJavaClass());
        Set<AnnotatedMethod<? super T>> methods = annotatedType.getMethods();

        methods.stream()
                .filter(method -> method.isAnnotationPresent(Incoming.class) || method.isAnnotationPresent(Outgoing.class))
                .forEach(method -> collected.add(method.getJavaMember(), bean));
    }

    public <T> void analyze(Class<?> beanClass, Bean<T> bean) {
        LOGGER.info("Scanning type: " + beanClass);
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

    public CompletableFuture<Void> initializeAndRun() {
        if (initialized) {
            throw new IllegalStateException("MediatorManager was already initialized!");
        }
        LOGGER.info("Deployment done... start processing");

        // StreamRegistars are initialized asynchronously so we need to return a CompletableFuture and then block in ReactiveMessagingExtension
        return CompletableFuture.allOf(streamRegistars.stream()
                .map(StreamRegistar::initialize)
                .toArray(CompletableFuture[]::new))
                .thenAccept(v -> {
                    Set<String> unmanagedSubscribers = streamRegistry.getSubscriberNames();
                    LOGGER.info("Initializing mediators");
                    
                    collected.mediators()
                            .forEach(configuration -> {
                                
                                AbstractMediator mediator = createMediator(configuration);
                                
                                LOGGER.debug("Initializing {}", mediator.getMethodAsString());
                                
                                if (configuration.getInvokerClass() != null) {
                                    try {
                                        mediator.setInvoker(configuration.getInvokerClass()
                                                .newInstance());
                                    } catch (InstantiationException | IllegalAccessException e) {
                                        LOGGER.error("Unable to create invoker instance of " + configuration.getInvokerClass(), e);
                                        return;
                                    }
                                }
                                
                                try {
                                    Object beanInstance = beanManager.getReference(configuration.getBean(), Object.class, beanManager.createCreationalContext(configuration.getBean()));
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
                                    streamRegistry.register(mediator.getConfiguration()
                                            .getOutgoing(), mediator.getComputedPublisher());
                                }
                                if (mediator.getConfiguration()
                                        .shape() == Shape.SUBSCRIBER) {
                                    LOGGER.debug("Registering {} as subscriber {}", mediator.getConfiguration()
                                            .methodAsString(),
                                            mediator.getConfiguration()
                                                    .getIncoming());
                                    streamRegistry.register(mediator.getConfiguration()
                                            .getIncoming(), mediator.getComputedSubscriber());
                                }
                            });

                    weaving(unmanagedSubscribers);
                });
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
                List<Publisher<? extends Message>> sources = streamRegistry.getPublishers(mediator.configuration()
                        .getIncoming());
                Optional<Publisher<? extends Message>> maybeSource = getAggregatedSource(sources, mediator, lazy);
                maybeSource.ifPresent(publisher -> {
                    mediator.connectToUpstream(publisher);
                    LOGGER.info("Connecting {} to `{}` ({})", mediator.getMethodAsString(), mediator.configuration()
                            .getIncoming(), publisher);
                    if (mediator.configuration()
                            .getOutgoing() != null) {
                        streamRegistry.register(mediator.getConfiguration()
                                .getOutgoing(), mediator.getComputedPublisher());
                    }
                });
            });

            unsatisfied = getAllNonSatisfiedMediators();
            int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

            if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
                // Stale!
                if (strictMode) {
                    throw new WeavingException("Impossible to bind mediators, some mediators are not connected: " + unsatisfied.stream()
                            .map(m -> m.configuration()
                                    .methodAsString())
                            .collect(Collectors.toList()) + ", available publishers:" + streamRegistry.getPublisherNames());
                } else {
                    LOGGER.warn("Impossible to bind mediators, some mediators are not connected: {}", unsatisfied.stream()
                            .map(m -> m.configuration()
                                    .methodAsString())
                            .collect(Collectors.toList()));
                    LOGGER.warn("Available publishers: {}", streamRegistry.getPublisherNames());
                }
                break;
            }
        }

        // Inject lazy sources
        lazy.forEach(l -> l.configure(streamRegistry, LOGGER));

        // Run
        mediators.stream()
                .filter(m -> m.configuration()
                        .shape() == Shape.SUBSCRIBER)
                .filter(AbstractMediator::isConnected)
                .forEach(AbstractMediator::run);

        // We also need to connect mediator to un-managed subscribers
        for (String name : unmanagedSubscribers) {
            List<AbstractMediator> list = lookupForMediatorsWithMatchingDownstream(name);
            for (AbstractMediator mediator : list) {
                List<Subscriber<? extends Message>> subscribers = streamRegistry.getSubscribers(name);

                if (subscribers.size() == 1) {
                    LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
                    mediator.getComputedPublisher()
                            .subscribe((Subscriber) subscribers.get(0));
                } else if (subscribers.size() > 2) {
                    LOGGER.warn("{} subscribers consuming the stream {}", subscribers.size(), name);
                    subscribers.forEach(s -> {
                        LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
                        mediator.getComputedPublisher()
                                .subscribe((Subscriber) s);
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

    private Optional<Publisher<? extends Message>> getAggregatedSource(List<Publisher<? extends Message>> sources, AbstractMediator mediator,
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
            return Optional.of(lazySource);
        }

        if (sources.size() > 1) {
            throw new WeavingException(mediator.configuration()
                    .getIncoming(), mediator.getMethodAsString(), sources.size());
        }
        return Optional.of(sources.get(0));

    }

}
