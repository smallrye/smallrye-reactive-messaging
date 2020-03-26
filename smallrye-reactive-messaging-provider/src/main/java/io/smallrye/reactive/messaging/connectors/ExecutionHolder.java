package io.smallrye.reactive.messaging.connectors;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
public class ExecutionHolder {

    @Inject
    private Instance<Vertx> instanceOfVertx;

    private boolean internalVertxInstance = false;
    private Vertx vertx;

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(200) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (internalVertxInstance) {
            vertx.close().await().indefinitely();
        }
    }

    @PostConstruct
    public void init() {
        if (instanceOfVertx == null || instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else {
            this.vertx = instanceOfVertx.get();
        }
    }

    public Vertx vertx() {
        return vertx;
    }
}
