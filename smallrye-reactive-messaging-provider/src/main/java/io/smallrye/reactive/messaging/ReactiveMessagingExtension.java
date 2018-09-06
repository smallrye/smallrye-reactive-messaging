package io.smallrye.reactive.messaging;

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

public class ReactiveMessagingExtension implements Extension {

  private static final Logger LOGGER = LogManager.getLogger(ReactiveMessagingExtension.class);

  private Vertx vertx;
  private boolean hasVertxBeenInitializedHere = false;

  private final Collected collected = new Collected();
  private StreamRegistry registry;
  private MediatorFactory factory;
  private final List<AbstractMediator> mediators = new ArrayList<>();


  <T> void processAnnotatedType(@Observes @WithAnnotations({Incoming.class, Outgoing.class}) ProcessAnnotatedType<T> pat) {
    LOGGER.info("scanning type: " + pat.getAnnotatedType().getJavaClass().getName());
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
    if (hasVertxBeenInitializedHere) {
      LOGGER.info("Closing vert.x");
      vertx.close();
    }
  }

  void afterBeanDiscovery(@Observes AfterDeploymentValidation done, BeanManager beanManager) {
    LOGGER.info("Deployment done... start processing");
    getOrCreateVertxInstance(beanManager);
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
        Optional<Publisher<? extends Message>> maybeSource = lookForSource(mediator);
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
        LOGGER.warn("Impossible to weave mediators, some mediators are not connected: {}",
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
        Subscriber subscriber = registry.getSubscriber(name)
          .orElseThrow(() -> new IllegalStateException("Did the subscriber just left? " + name));
        mediator.getComputedPublisher().subscribe(subscriber);
      }
    }
  }

  private List<AbstractMediator> lookupForMediatorsWithMatchingDownstream(String name) {
    return mediators.stream()
      .filter(m -> m.configuration.getOutgoing() != null)
      .filter(m -> m.configuration.getOutgoing().equalsIgnoreCase(name))
      .collect(Collectors.toList());
  }

  private Optional<Publisher<? extends Message>> lookForSource(AbstractMediator mediator) {
    String incoming = mediator.configuration.getIncoming();
    return registry.getPublisher(incoming);
  }

  private List<AbstractMediator> getAllNonSatisfiedMediators() {
    return mediators.stream().filter(mediator -> !mediator.isConnected()).collect(Collectors.toList());
  }

  private void createMediator(MediatorConfiguration configuration) {
    AbstractMediator mediator = factory.create(configuration);
    LOGGER.info("Mediator created for {}", configuration.methodAsString());
    mediators.add(mediator);
  }

  private void getOrCreateVertxInstance(BeanManager beanManager) {
    // TODO It would be great to externalize the management of the Vert.x instance and allow the instance to be injected.
    this.vertx = beanManager.createInstance().select(Vertx.class).stream().findFirst().orElseGet(() -> {
      hasVertxBeenInitializedHere = true;
      return Vertx.vertx();
    });
    LOGGER.debug("Vert.x instance: " + vertx);
  }

  public Vertx vertx() {
    return vertx;
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
