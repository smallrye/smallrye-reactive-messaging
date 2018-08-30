package io.smallrye.reactive.messaging;

import io.vertx.reactivex.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ReactiveMessagingExtension implements Extension {

  private static final Logger LOGGER = LogManager.getLogger(ReactiveMessagingExtension.class);

  private Vertx vertx;
  private boolean hasVertxBeenInitializedHere = false;

  private final Collected collected = new Collected();
  private StreamRegistry registry;
  private MediatorFactory factory;
  private final List<Mediator> mediators = new ArrayList<>();


  <T> void processAnnotatedType(@Observes @WithAnnotations({Incoming.class, Outgoing.class}) ProcessAnnotatedType<T> pat) {
    LOGGER.info("scanning type: " + pat.getAnnotatedType().getJavaClass().getName());
    Set<AnnotatedMethod<? super T>> methods = pat.getAnnotatedType().getMethods();
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Incoming.class))
      .forEach(method -> collected.add(pat, method.getJavaMember()));

    // For method with only @Outgoing
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Outgoing.class))
      .filter(method -> ! method.isAnnotationPresent(Incoming.class))
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
        collected.mediators.forEach(this::createMediator);

        LOGGER.info("Initializing mediators");
        mediators.forEach(mediator -> {
          LOGGER.debug("Initializing {}", mediator.getConfiguration().methodAsString());
          mediator.initialize(instance.select(mediator.getConfiguration().getBeanClass()).get());

          if (mediator.getConfiguration().isPublisher()) {
            LOGGER.debug("Registering {} as publisher {}", mediator.getConfiguration().methodAsString(), mediator.getConfiguration().getOutgoing());
            registry.register(mediator.getConfiguration().getOutgoing(), mediator.getOutput());
          }
          if (mediator.getConfiguration().isSubscriber()) {
            LOGGER.debug("Registering {} as subscriber {}", mediator.getConfiguration().methodAsString(), mediator.getConfiguration().getIncoming());
            registry.register(mediator.getConfiguration().getIncoming(), mediator.getInput());
          }
        });

        try {
          connectTheMediators();
        } catch (Exception e) {
          done.addDeploymentProblem(e);
        }
      });
  }

  private void connectTheMediators() {
    LOGGER.info("Connecting mediators");
    // TODO Warnings when source of sinks are not found.
    for (Mediator mediator : mediators) {
      if (mediator.getConfiguration().isSubscriber()) {
        registry.getPublisher(mediator.getConfiguration().getIncoming()).ifPresent(mediator::connect);
      }
    }

    for (Mediator mediator : mediators) {
      if (mediator.getConfiguration().isPublisher()) {
        // Search for the sink
        registry.getSubscriber(mediator.getConfiguration().getOutgoing()).ifPresent(mediator::connect);
      }
    }
  }

  private void createMediator(MediatorConfiguration configuration) {
    Mediator mediator = factory.create(configuration);
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
      return new MediatorConfiguration(met, pat.getAnnotatedType().getJavaClass())
        .setIncoming(met.getAnnotation(Incoming.class))
        .setOutgoing(met.getAnnotation(Outgoing.class));
    }
  }
}
