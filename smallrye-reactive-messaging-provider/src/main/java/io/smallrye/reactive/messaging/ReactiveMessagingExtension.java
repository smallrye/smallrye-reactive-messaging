package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.annotations.Stream;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.PublisherInjectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.*;
import javax.inject.Named;

import static io.smallrye.reactive.messaging.impl.VertxBeanRegistration.registerVertxBeanIfNeeded;

public class ReactiveMessagingExtension implements Extension {

  private static final Logger LOGGER = LogManager.getLogger(ReactiveMessagingExtension.class);

  private MediatorManager mediators = new MediatorManager();
  private PublisherInjectionManager injections = new PublisherInjectionManager();


  <T> void processClassesContainingMediators(@Observes @WithAnnotations({Incoming.class, Outgoing.class}) ProcessAnnotatedType<T> pat) {
    mediators.analyze(pat);
  }

  <T> void processClassesRequestingPublisher(@Observes @WithAnnotations({Stream.class}) ProcessAnnotatedType<T> pat) {
    injections.analyze(pat);
  }

  void shutdown(@Observes BeforeShutdown bs) {
    mediators.shutdown();
  }

  /**
   * In this callback, regular beans have been found, we can declare new beans.
   * @param discovery the discovery event
   * @param beanManager the bean manager
   */
  void afterBeanDiscovery(@Observes AfterBeanDiscovery discovery, BeanManager beanManager) {
    registerVertxBeanIfNeeded(discovery, beanManager);
    LOGGER.info("Creating synthetic beans for injection points");
    injections.createBeans(discovery);

  }

  void afterDeploymentValidation(@Observes AfterDeploymentValidation done, BeanManager beanManager) {
    StreamRegistry registry = beanManager.createInstance().select(StreamRegistry.class).get();
    injections.setRegistry(registry);
    mediators.initializeAndRun(beanManager, registry, done);
  }

  public boolean isInitialized() {
    return mediators.isInitialized();
  }
}
