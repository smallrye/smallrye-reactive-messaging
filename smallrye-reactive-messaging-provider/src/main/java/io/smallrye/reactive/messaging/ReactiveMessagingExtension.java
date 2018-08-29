package io.smallrye.reactive.messaging;

import io.vertx.reactivex.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.SubscriberBuilder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.*;
import javax.enterprise.util.TypeLiteral;
import javax.inject.Named;
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

  <X extends Publisher<? extends Message<?>>> void wrapPublisherProducer(@Observes ProcessProducer<?, X> pp, BeanManager bm) {
    if (pp.getAnnotatedMember().isAnnotationPresent(Named.class)) {
      String name = pp.getAnnotatedMember().getAnnotation(Named.class).value();
      LOGGER.info("Found a publisher producer named: " + name);

      Producer<X> oldProducer = pp.getProducer();

      pp.configureProducer().produceWith(creationalContext -> {
        StreamRegistry reg = getRegistry(bm);
        if (reg.getPublisherNames().contains(name)) {
          LOGGER.warn("Found the Publisher producer named '{}', however this name was already used by {}", name, reg.getPublisher(name).toString());
        }
        reg.register(name, oldProducer.produce(creationalContext));
        return (X) reg.getPublisher(name).get();
      });
    }
  }

  <X extends PublisherBuilder<? extends Message>> void wrapPublisherBuilderProducer(@Observes ProcessProducer<?, X> pp, BeanManager bm) {
    if (pp.getAnnotatedMember().isAnnotationPresent(Named.class)) {
      String name = pp.getAnnotatedMember().getAnnotation(Named.class).value();
      LOGGER.info("Found a publisher builder producer named: " + name);

      Producer<X> oldProducer = pp.getProducer();

      pp.configureProducer().produceWith(creationalContext -> {
        StreamRegistry reg = getRegistry(bm);
        X res = oldProducer.produce(creationalContext);

        if (reg.getPublisherNames().contains(name)) {
          LOGGER.warn("Found the Publisher producer (as a builder) named '{}', however this name was already used by {}", name, reg.getPublisher(name).toString());
        }
        reg.register(name, res.buildRs());
        return res;
      });
    }
  }

  <X extends Subscriber<? extends Message<?>>> void wrapSubscriberProducer(@Observes ProcessProducer<?, X> pp, BeanManager bm) {
    if (pp.getAnnotatedMember().isAnnotationPresent(Named.class)) {
      String name = pp.getAnnotatedMember().getAnnotation(Named.class).value();
      LOGGER.info("Found a subscriber producer named: " + name);

      Producer<X> oldProducer = pp.getProducer();

      pp.configureProducer().produceWith(creationalContext -> {
        StreamRegistry reg = getRegistry(bm);
        if (reg.getSubscriberNames().contains(name)) {
          LOGGER.warn("Found the Subscriber producer named '{}', however this name was already used by {}", name, reg.getSubscriber(name).toString());
        }
        reg.register(name, oldProducer.produce(creationalContext));
        return (X) reg.getSubscriber(name).get();
      });
    }
  }

  <X extends SubscriberBuilder<? extends Message, ?>> void wrapSubscriberBuilderProducer(@Observes ProcessProducer<?, X> pp, BeanManager bm) {
    if (pp.getAnnotatedMember().isAnnotationPresent(Named.class)) {
      String name = pp.getAnnotatedMember().getAnnotation(Named.class).value();
      LOGGER.info("Found a subscriber builder producer named: " + name);

      Producer<X> oldProducer = pp.getProducer();

      pp.configureProducer().produceWith(creationalContext -> {
        StreamRegistry reg = getRegistry(bm);
        X res = oldProducer.produce(creationalContext);

        if (reg.getSubscriberNames().contains(name)) {
          LOGGER.warn("Found the Subscriber producer (as a builder) named '{}', however this name was already used by {}", name, reg.getSubscriber(name).toString());
        }
        reg.register(name, res.build());
        return res;
      });
    }
  }

  private StreamRegistry getRegistry(BeanManager bm) {
    if (registry == null) {
      registry = bm.createInstance().select(StreamRegistry.class).stream().findFirst()
        .orElseThrow(() -> new IllegalStateException("Unable to find the " + StreamRegistry.class.getName() + " component"));
    }
    return registry;
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

        //requesting an instance for each Publisher and Subscriber bean

        instance.select(new TypeLiteral<Publisher<?>>() {
        }).forEach(d -> {
        });
        instance.select(new TypeLiteral<PublisherBuilder<?>>() {
        }).forEach(d -> {
        });
        instance.select(new TypeLiteral<Subscriber<?>>() {
        }).forEach(d -> {
        });
        instance.select(new TypeLiteral<SubscriberBuilder<?, ?>>() {
        }).forEach(d -> {
        });

        collected.mediators.forEach(this::createMediator);

        mediators.forEach(mediator -> {
          mediator.initialize(beanManager.createInstance().select(mediator.getConfiguration().getBeanClass()).get());
          if (mediator.getConfiguration().isPublisher()) {
            registry.register(mediator.getConfiguration().getOutgoing(), mediator.getOutput());
          }
          if (mediator.getConfiguration().isSubscriber()) {
            registry.register(mediator.getConfiguration().getIncoming(), mediator.getInput());
          }
        });

        connectTheMediators();
      });
  }

  private void connectTheMediators() {
    for (Mediator mediator : mediators) {
      if (mediator.getConfiguration().isSubscriber()) {
        // Search for the source
        Publisher<? extends Message> publisher = registry.getPublisher(mediator.getConfiguration().getIncoming()).orElseThrow(() ->
          new RuntimeException("Unable to find the data stream named " + mediator.getConfiguration().getIncoming()));
        mediator.connect(publisher);
      }

      if (mediator.getConfiguration().isPublisher()) {
        // Search for the sink
        Subscriber<? extends Message> subscriber = registry.getSubscriber(mediator.getConfiguration().getOutgoing()).orElseThrow(() ->
          new RuntimeException("Unable to find the data stream named " + mediator.getConfiguration().getOutgoing()));
        mediator.connect(subscriber);
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
