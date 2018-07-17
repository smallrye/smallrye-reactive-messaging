package io.smallrye.reactive.messaging;

import io.reactivex.Flowable;
import io.vertx.reactivex.core.Vertx;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.*;
import javax.inject.Named;
import java.lang.reflect.Method;
import java.util.*;

public class ReactiveMessagingExtension implements Extension {

  private static final Logger LOGGER = LogManager.getLogger(ReactiveMessagingExtension.class);

  private Vertx vertx;
  private boolean hasVertxBeenInitializedHere = false;

  private Collected collected = new Collected();
  private StreamRegistry registry;
  private MediatorFactory factory;
  private List<Mediator> mediators = new ArrayList<>();


  <T> void processAnnotatedType(@Observes @WithAnnotations(Incoming.class) ProcessAnnotatedType<T> pat) {
    LOGGER.info("scanning type: " + pat.getAnnotatedType().getJavaClass().getName());
    Set<AnnotatedMethod<? super T>> methods = pat.getAnnotatedType().getMethods();
    methods.stream()
      .filter(method -> method.isAnnotationPresent(Incoming.class))
      .forEach(method -> collected.add(pat, method.getJavaMember()));
  }

  void shutdown(@Observes BeforeShutdown bs) {
    if (hasVertxBeenInitializedHere) {
      LOGGER.info("Closing vert.x");
      vertx.close();
    }
  }

  void collectPublisherAndSubscriberProducer(@Observes ProcessProducer producer) {
    // TODO extend class test to be sure they accept and produce messages
    boolean isPublisher = TypeUtils.isAssignable(producer.getAnnotatedMember().getBaseType(), Publisher.class);
    boolean isSubscriber = TypeUtils.isAssignable(producer.getAnnotatedMember().getBaseType(), Subscriber.class);
    if (producer.getAnnotatedMember().isAnnotationPresent(Named.class) && isPublisher) {
      LOGGER.info("Found a publisher producer named: " + producer.getAnnotatedMember().getAnnotation(Named.class).value());
      collected.addPublisher(producer.getAnnotatedMember().getAnnotation(Named.class).value(), producer.getProducer());
    }
    if (producer.getAnnotatedMember().isAnnotationPresent(Named.class) && isSubscriber) {
      LOGGER.info("Found a subscriber producer named: " + producer.getAnnotatedMember().getAnnotation(Named.class).value());
      collected.addSubscriber(producer.getAnnotatedMember().getAnnotation(Named.class).value(), producer.getProducer());
    }
  }

  void afterBeanDiscovery(@Observes AfterDeploymentValidation done, BeanManager beanManager) {
    LOGGER.info("Deployment done... start processing");
    getOrCreateVertxInstance(beanManager);
    this.registry = beanManager.createInstance().select(StreamRegistry.class).stream().findFirst()
      .orElseThrow(() -> new IllegalStateException("Unable to find the " + StreamRegistry.class.getName() + " component"));

    this.factory = beanManager.createInstance().select(MediatorFactory.class).stream().findFirst()
      .orElseThrow(() -> new IllegalStateException("Unable to find the " + MediatorFactory.class.getName() + " component"));

    collected.publisherProducers.forEach((name, producer) ->
      registry.register(name, (Publisher<? extends Message>) producer.produce(beanManager.createCreationalContext(null))));

    collected.subscriberProducers.forEach((name, producer) ->
      registry.register(name, (Subscriber<? extends Message>) producer.produce(beanManager.createCreationalContext(null))));

    collected.mediators.forEach((method, meta) -> createMediator(meta));

    mediators.forEach(mediator -> {
      mediator.initialize(beanManager.createInstance().select(mediator.getConfiguration().getBeanClass()).get());
      mediator.run();
    });
  }


  private void createMediator(MediatorConfiguration configuration) {
    Mediator mediator = factory.create(configuration);
    LOGGER.info("Mediator created for " + configuration.methodAsString());
    mediators.add(mediator);
    if (mediator.getConfiguration().getName() != null) {
      registry.register(mediator.getConfiguration().getName(), mediator.getPublisher());
    }
    // TODO Should we register it?
  }

  private void getOrCreateVertxInstance(BeanManager beanManager) {
    // TODO It would be great to externalize the management of the Vert.x instance and allow the instance to be injected.
    this.vertx = beanManager.createInstance().select(Vertx.class).stream().findFirst().orElseGet(() -> {
      hasVertxBeenInitializedHere = true;
      return Vertx.vertx();
    });
    LOGGER.info("Vert.x instance: " + vertx);
  }

  public Vertx vertx() {
    return vertx;
  }


  private class Collected {
    private Map<String, Producer> publisherProducers = new HashMap<>();
    private Map<String, Producer> subscriberProducers = new HashMap<>();

    // TODO shouldn't ist be a list
    private Map<Method, MediatorConfiguration> mediators = new HashMap<>();

    void addPublisher(String name, Producer producer) {
      if (publisherProducers.put(name, producer) != null) {
        LOGGER.warn("Found the Publisher producer named '{}', however this name was already used by {}", name, producer);
      }
    }

    void addSubscriber(String name, Producer producer) {
      if (subscriberProducers.put(name, producer) != null) {
        LOGGER.warn("Found the Subscriber producer named '{}', however this name was already used by {}", name, producer);
      }
    }

    void add(ProcessAnnotatedType pat, Method method) {
      mediators.put(method, createMediatorConfiguration(pat, method));
    }
  }

  private MediatorConfiguration createMediatorConfiguration(ProcessAnnotatedType pat, Method met) {
    return new MediatorConfiguration(met, pat.getAnnotatedType().getJavaClass())
      .setIncoming(met.getAnnotation(Incoming.class))
      .setOutgoing(met.getAnnotation(Outgoing.class))
      .setNamed(met.getAnnotation(Named.class));
  }


}
