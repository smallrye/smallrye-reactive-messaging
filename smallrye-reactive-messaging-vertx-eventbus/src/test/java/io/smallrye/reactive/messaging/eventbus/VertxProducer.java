package io.smallrye.reactive.messaging.eventbus;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import io.vertx.mutiny.core.Vertx;

public class VertxProducer {

    @Produces
    @ApplicationScoped
    Vertx vertx() {
        return Vertx.vertx();
    }
}
