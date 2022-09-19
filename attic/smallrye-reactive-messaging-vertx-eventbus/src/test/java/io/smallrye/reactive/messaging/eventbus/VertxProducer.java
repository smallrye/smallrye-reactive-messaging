package io.smallrye.reactive.messaging.eventbus;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import io.vertx.mutiny.core.Vertx;

public class VertxProducer {

    @Produces
    @ApplicationScoped
    Vertx vertx() {
        return Vertx.vertx();
    }
}
