package io.smallrye.reactive.messaging.impl;

import io.vertx.reactivex.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import java.util.Optional;

public class VertxBeanRegistration {

  public static final Logger LOGGER = LogManager.getLogger(VertxBeanRegistration.class);

  public static void registerVertxBeanIfNeeded(AfterBeanDiscovery discovery, BeanManager beanManager) {
    Optional<Bean<?>> optional = beanManager.getBeans(Vertx.class).stream().findFirst();
    if (! optional.isPresent()) {
      LOGGER.debug("Declaring a Vert.x bean");
      discovery.addBean()
        .types(Vertx.class)
        .beanClass(Vertx.class)
        .produceWith(i -> Vertx.vertx())
        .disposeWith((vertx, i) -> vertx.close());
    }
  }
}
