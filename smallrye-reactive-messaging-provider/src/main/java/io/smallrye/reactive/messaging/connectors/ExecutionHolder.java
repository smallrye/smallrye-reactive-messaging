package io.smallrye.reactive.messaging.connectors;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.slf4j.LoggerFactory;

import io.vertx.mutiny.core.Vertx;

/**
 * Provides common runtime services to connectors, such as a Vertx instance,
 * to avoid duplicating the creation and cleanup of shared components across connectors.
 */
@ApplicationScoped
public class ExecutionHolder {

    private boolean internalVertxInstance = false;
    final Vertx vertx;

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(200) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (internalVertxInstance) {
            vertx.close().await().indefinitely();
        }
    }

    // Dummy no-args constructor needed for client proxies, not needed on quarkus ;-)
    public ExecutionHolder() {
        this.vertx = null;
    }

    public ExecutionHolder(Vertx vertx) {
        this.vertx = vertx;
        internalVertxInstance = true;
    }

    @Inject
    public ExecutionHolder(Instance<Vertx> instanceOfVertx) {
        if (instanceOfVertx == null || instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
            LoggerFactory.getLogger(ExecutionHolder.class).info("Created new Vertx instance");
        } else {
            this.vertx = instanceOfVertx.get();
        }
    }

    public Vertx vertx() {
        return vertx;
    }
}
