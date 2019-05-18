package io.smallrye.reactive.messaging.eventbus;

import io.vertx.reactivex.core.Vertx;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

public class VertxProducer {

  @Produces
  @ApplicationScoped
  Vertx vertx() {
    return Vertx.vertx();
  }
}
