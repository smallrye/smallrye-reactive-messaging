package io.smallrye.reactive.messaging.providers.connectors;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

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
            log.vertXInstanceCreated();
        } else {
            this.vertx = instanceOfVertx.get();
        }
    }

    public Vertx vertx() {
        return vertx;
    }
}
