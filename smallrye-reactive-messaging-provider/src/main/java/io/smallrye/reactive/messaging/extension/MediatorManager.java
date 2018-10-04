package io.smallrye.reactive.messaging.extension;

import io.smallrye.reactive.messaging.*;
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
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Class responsible for managing mediators
 */
public class MediatorManager {

  private static final Logger LOGGER = LogManager.getLogger(MediatorManager.class);

  private final CollectedMediatorMetadata collected = new CollectedMediatorMetadata();
  // TODO Populate this list
  private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
  private final List<AbstractMediator> mediators = new ArrayList<>();

  private BeanManager manager;
  private StreamRegistry registry;
  private MediatorFactory factory;
  private boolean initialized;

  public <T> void analyze(ProcessAnnotatedType<T> pat) {
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
        } catch (Exception e) {
          done.addDeploymentProblem(e);
        }
      });
  }

  private void weaving(Set<String> unmanagedSubscribers) {
    // At that point all the publishers have been registered in the registry
    LOGGER.info("Connecting mediators");
    List<AbstractMediator> unsatisfied = getAllNonSatisfiedMediators();

    List<LazySource> lazy = new ArrayList<>();
    while (!unsatisfied.isEmpty()) {
      int numberOfUnsatisfiedBeforeLoop = unsatisfied.size();

      unsatisfied.forEach(mediator -> {
        LOGGER.info("Attempt to satisfied {}", mediator.getMethodAsString());
        List<Publisher<? extends Message>> sources = registry.getPublishers(mediator.configuration().getIncoming());
        Optional<Publisher<? extends Message>> maybeSource = getAggregatedSource(sources, mediator, lazy);
        maybeSource.ifPresent(publisher -> {
          mediator.connectToUpstream(publisher);
          LOGGER.info("Connecting {} to {} ({})", mediator.getMethodAsString(), mediator.configuration().getIncoming(), publisher);
          if (mediator.configuration().getOutgoing() != null) {
            registry.register(mediator.getConfiguration().getOutgoing(), mediator.getComputedPublisher());
          }
        });
      });

      unsatisfied = getAllNonSatisfiedMediators();
      int numberOfUnsatisfiedAfterLoop = unsatisfied.size();

      if (numberOfUnsatisfiedAfterLoop == numberOfUnsatisfiedBeforeLoop) {
        // Stale!
        LOGGER.warn("Impossible to bind mediators, some mediators are not connected: {}",
          unsatisfied.stream().map(m -> m.configuration().methodAsString()).collect(Collectors.toList()));
        LOGGER.warn("Available publishers: {}", registry.getPublisherNames());
        // TODO What should we do - break the deployment, report a warning?
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

    return Optional.of(sources.get(0));

  }

  public boolean isInitialized() {
    return initialized;
  }
}
