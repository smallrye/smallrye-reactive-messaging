package io.smallrye.reactive.messaging;

import io.smallrye.reactive.messaging.annotations.Stream;
import io.smallrye.reactive.messaging.extension.MediatorManager;
import io.smallrye.reactive.messaging.extension.PublisherInjectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeShutdown;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.enterprise.inject.spi.ProcessBean;
import javax.enterprise.inject.spi.WithAnnotations;

import static io.smallrye.reactive.messaging.impl.VertxBeanRegistration.registerVertxBeanIfNeeded;

public class ReactiveMessagingExtension implements Extension {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMessagingExtension.class);

  private MediatorManager mediators = new MediatorManager();
  private PublisherInjectionManager injections = new PublisherInjectionManager();


  <T> void processClassesContainingMediators(@Observes ProcessBean<T> bean) {
    if (bean.getAnnotated() instanceof AnnotatedType) {
      mediators.analyze((AnnotatedType<T>) bean.getAnnotated(), bean);
    }
  }

  <T> void processClassesRequestingPublisher(@Observes @WithAnnotations({Stream.class}) ProcessAnnotatedType<T> pat) {
    injections.analyze(pat);
  }

  void shutdown(@Observes BeforeShutdown bs) {
    mediators.shutdown();
  }

  /**
   * In this callback, regular beans have been found, we can declare new beans.
   *
   * @param discovery   the discovery event
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
