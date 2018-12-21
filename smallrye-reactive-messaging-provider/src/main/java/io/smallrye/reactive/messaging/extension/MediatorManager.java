package io.smallrye.reactive.messaging.extension;

import io.smallrye.reactive.messaging.AbstractMediator;
import io.smallrye.reactive.messaging.MediatorConfiguration;
import io.smallrye.reactive.messaging.MediatorFactory;
import io.smallrye.reactive.messaging.Shape;
import io.smallrye.reactive.messaging.StreamRegistar;
import io.smallrye.reactive.messaging.StreamRegistry;
import io.smallrye.reactive.messaging.WeavingException;
import io.smallrye.reactive.messaging.annotations.Merge;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.ProcessBean;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Class responsible for managing mediators
 */
public class MediatorManager {

  private static final Logger LOGGER = LogManager.getLogger(MediatorManager.class);
  public static final String STRICT_MODE_PROPERTY = "smallrye-messaging-strict-binding";
  private final boolean strictMode;

  private final CollectedMediatorMetadata collected = new CollectedMediatorMetadata();
  // TODO Populate this list
  private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
  private final List<AbstractMediator> mediators = new ArrayList<>();

  private BeanManager manager;
  private StreamRegistry registry;
  private MediatorFactory factory;
  private boolean initialized;

  public MediatorManager() {
    strictMode = Boolean.parseBoolean(System.getProperty(STRICT_MODE_PROPERTY, "false"));
    if (strictMode) {
      LOGGER.debug("Strict mode enabled");
    }
  }

  public <T> void analyze(AnnotatedType<T> type, ProcessBean<T> bean) {
    LOGGER.info("Scanning Type: " + type.getJavaClass().getName());
    Set<AnnotatedMethod<? super T>> methods = type.getMethods();

    methods.stream()
      .filter(method -> method.isAnnotationPresent(Incoming.class))
      .forEach(method -> collected.add(type, method.getJavaMember(), bean.getBean().getQualifiers()));

    // For method with only @Outgoing
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Outgoing.class))
      .filter(method -> !method.isAnnotationPresent(Incoming.class))
      .forEach(method -> collected.add(type, method.getJavaMember(), bean.getBean().getQualifiers()));
  }

  public void shutdown() {
    subscriptions.forEach(Subscription::cancel);
    subscriptions.clear();
  }

  public void initializeAndRun(BeanManager beanManager, StreamRegistry registry, AfterDeploymentValidation done) {
    this.manager = beanManager;
    this.registry = registry;
    this.factory = lookup(MediatorFactory.class);

    LOGGER.info("Deployment done... start processing");

    Collection<StreamRegistar> registars = beanManager.createInstance().select(StreamRegistar.class).stream().collect(Collectors.toList());

    Instance<Object> instance = beanManager.createInstance();

    CompletableFuture.allOf(
      registars.stream().map(StreamRegistar::initialize).toArray(CompletableFuture[]::new)
    )
      .thenAccept(v -> {
        try {
          Set<String> unmanagedSubscribers = registry.getSubscriberNames();
          collected.mediators().forEach(this::createMediator);

          LOGGER.info("Initializing mediators");
          mediators.forEach(mediator -> {
            LOGGER.debug("Initializing {}", mediator.getMethodAsString());
            try {
              mediator.initialize(instance.select(mediator.getConfiguration().getBeanClass(), mediator.getConfiguration().getQualifiers().toArray(new Annotation[0])).get());
            } catch (Throwable e) {
              LOGGER.fatal("Unable to initialize mediator {}", mediator.getMethodAsString(), e);
              return;
            }

            if (mediator.getConfiguration().shape() == Shape.PUBLISHER) {
              LOGGER.debug("Registering {} as publisher {}", mediator.getConfiguration().methodAsString(), mediator.getConfiguration().getOutgoing());
              registry.register(mediator.getConfiguration().getOutgoing(), mediator.getComputedPublisher());
            }
            if (mediator.getConfiguration().shape() == Shape.SUBSCRIBER) {
              LOGGER.debug("Registering {} as subscriber {}", mediator.getConfiguration().methodAsString(), mediator.getConfiguration().getIncoming());
              registry.register(mediator.getConfiguration().getIncoming(), mediator.getComputedSubscriber());
            }
          });

          weaving(unmanagedSubscribers);
        } catch (Exception e) {
          done.addDeploymentProblem(e);
        }
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
        List<Publisher<? extends Message>> sources = registry.getPublishers(mediator.configuration().getIncoming());
        Optional<Publisher<? extends Message>> maybeSource = getAggregatedSource(sources, mediator, lazy);
        maybeSource.ifPresent(publisher -> {
          mediator.connectToUpstream(publisher);
          LOGGER.info("Connecting {} to `{}` ({})", mediator.getMethodAsString(), mediator.configuration().getIncoming(), publisher);
          if (mediator.configuration().getOutgoing() != null) {
            registry.register(mediator.getConfiguration().getOutgoing(), mediator.getComputedPublisher());
          }
        });
      });

      unsatisfied = getAllNonSatisfiedMediators();
      int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

      if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
        // Stale!
        if (strictMode) {
          throw new WeavingException("Impossible to bind mediators, some mediators are not connected: " +
            unsatisfied.stream().map(m -> m.configuration().methodAsString()).collect(Collectors.toList()) +
            ", available publishers:" + registry.getPublisherNames());
        } else {
          LOGGER.warn("Impossible to bind mediators, some mediators are not connected: {}",
            unsatisfied.stream().map(m -> m.configuration().methodAsString()).collect(Collectors.toList()));
          LOGGER.warn("Available publishers: {}", registry.getPublisherNames());
        }
        break;
      }
    }

    // Inject lazy sources
    lazy.forEach(l -> l.configure(registry, LOGGER));

    // Run
    mediators.stream().filter(m -> m.configuration().shape() == Shape.SUBSCRIBER)
      .filter(AbstractMediator::isConnected)
      .forEach(AbstractMediator::run);

    // We also need to connect mediator to un-managed subscribers
    for (String name : unmanagedSubscribers) {
      List<AbstractMediator> list = lookupForMediatorsWithMatchingDownstream(name);
      for (AbstractMediator mediator : list) {
        List<Subscriber<? extends Message>> subscribers = registry.getSubscribers(name);

        if (subscribers.size() == 1) {
          LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
          mediator.getComputedPublisher().subscribe((Subscriber) subscribers.get(0));
        } else if (subscribers.size() > 2) {
          LOGGER.warn("{} subscribers consuming the stream {}", subscribers.size(), name);
          subscribers.forEach(s -> {
            LOGGER.info("Connecting method {} to sink {}", mediator.getMethodAsString(), name);
            mediator.getComputedPublisher().subscribe((Subscriber) s);
          });
        }
      }
    }

    initialized = true;
  }

  private List<AbstractMediator> lookupForMediatorsWithMatchingDownstream(String name) {
    return mediators.stream()
      .filter(m -> m.configuration().getOutgoing() != null)
      .filter(m -> m.configuration().getOutgoing().equalsIgnoreCase(name))
      .collect(Collectors.toList());
  }

  private List<AbstractMediator> getAllNonSatisfiedMediators() {
    return mediators.stream().filter(mediator -> !mediator.isConnected()).collect(Collectors.toList());
  }

  private void createMediator(MediatorConfiguration configuration) {
    AbstractMediator mediator = factory.create(configuration);
    LOGGER.info("Mediator created for {}", configuration.methodAsString());
    mediators.add(mediator);
  }


  private <T> T lookup(Class<T> beanClass) {
    return manager.createInstance().select(beanClass).stream().findAny()
      .orElseThrow(() -> new IllegalStateException("Unable to find the " + beanClass.getName() + " component"));
  }


  private Optional<Publisher<? extends Message>> getAggregatedSource(List<Publisher<? extends Message>> sources,
                                                                     AbstractMediator mediator,
                                                                     List<LazySource> lazy) {
    if (sources.isEmpty()) {
      return Optional.empty();
    }

    Merge.Mode merge = mediator.getConfiguration().getMerge();
    if (merge != null) {
      LazySource lazySource = new LazySource(mediator.configuration().getIncoming(), merge);
      lazy.add(lazySource);
      return Optional.of(lazySource);
    }

    if (sources.size() > 1) {
      throw new WeavingException(mediator.configuration().getIncoming(), mediator.getMethodAsString(), sources.size());
    }
    return Optional.of(sources.get(0));

  }

  public boolean isInitialized() {
    return initialized;
  }
}
