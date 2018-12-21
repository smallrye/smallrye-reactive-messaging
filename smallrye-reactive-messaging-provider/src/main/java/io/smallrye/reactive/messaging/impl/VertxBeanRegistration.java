package io.smallrye.reactive.messaging.impl;

import io.vertx.reactivex.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Singleton;
import java.util.Optional;

public class VertxBeanRegistration {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertxBeanRegistration.class);

  private VertxBeanRegistration() {
    // Avoid direct instantiation.
  }

  public static void registerVertxBeanIfNeeded(AfterBeanDiscovery discovery, BeanManager beanManager) {
    Optional<Bean<?>> optional = beanManager.getBeans(Vertx.class).stream().findFirst();
    if (!optional.isPresent()) {
      LOGGER.debug("Declaring a Vert.x bean");
      discovery.addBean()
        .types(Vertx.class)
        .beanClass(Vertx.class)
        .scope(Singleton.class)
        .produceWith(i -> Vertx.vertx())
        .disposeWith((vertx, i) -> vertx.close());
    }
  }
}
