package io.smallrye.reactive.messaging.providers.connectors;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderLogging.log;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;

import io.smallrye.common.annotation.Identifier;
import io.vertx.mutiny.core.Vertx;

/**
 * Provides common runtime services to connectors, such as a Vertx instance,
 * to avoid duplicating the creation and cleanup of shared components across connectors.
 */
@ApplicationScoped
public class ExecutionHolder {

    private static final String REACTIVE_MESSAGING_VERTX_CDI_QUALIFIER = "mp.messaging.connector.vertx.cdi.identifier";

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
    public ExecutionHolder(@Any Instance<Vertx> instanceOfVertx, Instance<Config> config) {
        String cdiQualifier = null;
        if (config != null && !config.isUnsatisfied()) {
            final Config theConfig = config.get();
            cdiQualifier = theConfig.getConfigValue(REACTIVE_MESSAGING_VERTX_CDI_QUALIFIER).getValue();
        }
        final Instance<Vertx> vertxInstance;
        if (cdiQualifier != null && !cdiQualifier.isEmpty()) {
            log.vertxFromCDIQualifier(cdiQualifier);
            vertxInstance = instanceOfVertx.select(Identifier.Literal.of(cdiQualifier));
        } else {
            vertxInstance = instanceOfVertx.select(Default.Literal.INSTANCE);
        }
        if (vertxInstance == null || vertxInstance.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
            log.vertXInstanceCreated();
        } else {
            this.vertx = vertxInstance.get();
        }
    }

    public Vertx vertx() {
        return vertx;
    }

    // this is used in the test
    boolean isInternalVertxInstance() {
        return internalVertxInstance;
    }
}
