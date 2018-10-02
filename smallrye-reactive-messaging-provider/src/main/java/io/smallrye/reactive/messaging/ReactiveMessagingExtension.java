package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.vertx.reactivex.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.smallrye.reactive.messaging.impl.VertxBeanRegistration.registerVertxBeanIfNeeded;

public class ReactiveMessagingExtension implements Extension {

  private static final Logger LOGGER = LogManager.getLogger(ReactiveMessagingExtension.class);

  private final Collected collected = new Collected();
  private StreamRegistry registry;
  private MediatorFactory factory;
  private final List<AbstractMediator> mediators = new ArrayList<>();
  private boolean initialized;


  public boolean isInitialized() {
    return initialized;
  }

  <T> void processAnnotatedType(@Observes @WithAnnotations({Incoming.class, Outgoing.class}) ProcessAnnotatedType<T> pat) {
    LOGGER.info("Scanning Type: " + pat.getAnnotatedType().getJavaClass().getName());
    Set<AnnotatedMethod<? super T>> methods = pat.getAnnotatedType().getMethods();
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Incoming.class))
      .forEach(method -> collected.add(pat, method.getJavaMember()));

    // For method with only @Outgoing
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Outgoing.class))
      .filter(method -> !method.isAnnotationPresent(Incoming.class))
      .forEach(method -> collected.add(pat, method.getJavaMember()));
  }

  void shutdown(@Observes BeforeShutdown bs) {
    // TODO cleanup all subscriptions.
  }

  /**
   * In this callback, regular beans have been found, we can declare new beans.
   * @param discovery the discovery event
   * @param beanManager the bean manager
   */
  void afterBeanDiscovery(@Observes AfterBeanDiscovery discovery, BeanManager beanManager) {
    registerVertxBeanIfNeeded(discovery, beanManager);


  }

  void afterBeanDiscovery(@Observes AfterDeploymentValidation done, BeanManager beanManager) {
    LOGGER.info("Deployment done... start processing");

    this.registry = beanManager.createInstance().select(StreamRegistry.class).stream().findFirst()
      .orElseThrow(() -> new IllegalStateException("Unable to find the " + StreamRegistry.class.getName() + " component"));

    this.factory = beanManager.createInstance().select(MediatorFactory.class).stream().findFirst()
      .orElseThrow(() -> new IllegalStateException("Unable to find the " + MediatorFactory.class.getName() + " component"));

    Collection<StreamRegistar> registars = beanManager.createInstance().select(StreamRegistar.class).stream().collect(Collectors.toList());

    Instance<Object> instance = beanManager.createInstance();

    CompletableFuture.allOf(
      registars.stream().map(StreamRegistar::initialize).toArray(CompletableFuture[]::new)
    )
      .thenAccept(v -> {
        try {
          Set<String> unmanagedSubscribers = registry.getSubscriberNames();
          collected.mediators.forEach(this::createMediator);

          LOGGER.info("Initializing mediators");
          mediators.forEach(mediator -> {
            LOGGER.debug("Initializing {}", mediator.getMethodAsString());
            mediator.initialize(instance.select(mediator.getConfiguration().getBeanClass()).get());

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
          this.initialized = true;
        } catch (Exception e) {
          done.addDeploymentProblem(e);
        }
      });
  }

  private void weaving(Set<String> unmanagedSubscribers) {
    LOGGER.info("Connecting mediators");
    // At that point all the publisher have been registered in the registry

    List<AbstractMediator> unsatisfied = getAllNonSatisfiedMediators();

    while (!unsatisfied.isEmpty()) {
      int numberOfUnsatisfiedBeforeLoop = unsatisfied.size();

      unsatisfied.forEach(mediator -> {
        LOGGER.info("Attempt to satisfied {}", mediator.getMethodAsString());
        List<Publisher<? extends Message>> sources = lookForSource(mediator);
        Optional<Publisher<? extends Message>> maybeSource = getAggregatedSource(sources, mediator);
        maybeSource.ifPresent(publisher -> {
          mediator.connectToUpstream(publisher);
          LOGGER.info("Connecting {} to {} ({})", mediator.getMethodAsString(), mediator.configuration.getIncoming(), publisher);
          if (mediator.configuration.getOutgoing() != null) {
            registry.register(mediator.getConfiguration().getOutgoing(), mediator.getComputedPublisher());
          }
        });
      });

      unsatisfied = getAllNonSatisfiedMediators();
      int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

      if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
        // Stale!
        LOGGER.warn("Impossible to bind mediators, some mediators are not connected: {}",
          unsatisfied.stream().map(m -> m.configuration.methodAsString()).collect(Collectors.toList()));
        LOGGER.warn("Available publishers: {}", registry.getPublisherNames());
        // TODO What should we do - break the deployment, report a warning?
        break;
      }
    }

    // Run
    mediators.stream().filter(m -> m.configuration.shape() == Shape.SUBSCRIBER)
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
  }

  private Optional<Publisher<? extends Message>> getAggregatedSource(List<Publisher<? extends Message>> sources, AbstractMediator mediator) {
    if (sources.isEmpty()) {
      return Optional.empty();
    }

    if (sources.size() == 1) {
      return Optional.of(sources.get(0));
    }

    Merge.Mode merge = mediator.getConfiguration().getMerge();
    if (merge == null) {
      LOGGER.warn("Applying merge policy for {}, {} sources found",mediator.getMethodAsString(), sources.size());
      merge = Merge.Mode.MERGE;
    }

    switch (merge) {
      case MERGE:
        return Optional.of(Flowable.merge(sources));
      case CONCAT:
        return Optional.of(Flowable.concat(sources));
      case ONE:
        LOGGER.warn("Using the `ONE` merge strategy with {} sources for {}", sources.size(), mediator.getMethodAsString());
        return Optional.of(sources.get(0));
        default: throw new IllegalStateException("Unknown merge policy: " + merge);
    }
  }

  private List<AbstractMediator> lookupForMediatorsWithMatchingDownstream(String name) {
    return mediators.stream()
      .filter(m -> m.configuration.getOutgoing() != null)
      .filter(m -> m.configuration.getOutgoing().equalsIgnoreCase(name))
      .collect(Collectors.toList());
  }

  private List<Publisher<? extends Message>> lookForSource(AbstractMediator mediator) {
    String incoming = mediator.configuration.getIncoming();
    return registry.getPublishers(incoming);
  }

  private List<AbstractMediator> getAllNonSatisfiedMediators() {
    return mediators.stream().filter(mediator -> !mediator.isConnected()).collect(Collectors.toList());
  }

  private void createMediator(MediatorConfiguration configuration) {
    AbstractMediator mediator = factory.create(configuration);
    LOGGER.info("Mediator created for {}", configuration.methodAsString());
    mediators.add(mediator);
  }

  private class Collected {

    private final List<MediatorConfiguration> mediators = new ArrayList<>();

    void add(ProcessAnnotatedType pat, Method method) {
      mediators.add(createMediatorConfiguration(pat, method));
    }

    private MediatorConfiguration createMediatorConfiguration(ProcessAnnotatedType pat, Method met) {
      MediatorConfiguration configuration = new MediatorConfiguration(met, pat.getAnnotatedType().getJavaClass());
      configuration.compute(met.getAnnotation(Incoming.class), met.getAnnotation(Outgoing.class));
      return configuration;
    }
  }
}
