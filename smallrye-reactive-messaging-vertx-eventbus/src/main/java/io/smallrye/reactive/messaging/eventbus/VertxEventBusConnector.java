package io.smallrye.reactive.messaging.eventbus;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(VertxEventBusConnector.CONNECTOR_NAME)
public class VertxEventBusConnector implements OutgoingConnectorFactory, IncomingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-vertx-eventbus";

    @Inject
    Instance<Vertx> instanceOfVertx;

    private boolean internalVertxInstance = false;
    private Vertx vertx;

    public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (internalVertxInstance) {
            vertx.close();
        }
    }

    @PostConstruct
    void init() {
        if (instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else {
            this.vertx = instanceOfVertx.get();
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        return new EventBusSource(vertx, config).source();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        return new EventBusSink(vertx, config).sink();
    }

}
