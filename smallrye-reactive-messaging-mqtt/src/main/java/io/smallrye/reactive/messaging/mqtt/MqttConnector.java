package io.smallrye.reactive.messaging.mqtt;

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

import io.vertx.reactivex.core.Vertx;

@ApplicationScoped
@Connector(MqttConnector.CONNECTOR_NAME)
public class MqttConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-mqtt";

    @Inject
    private Instance<Vertx> instanceOfVertx;

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
    public PublisherBuilder<? extends Message> getPublisherBuilder(Config config) {
        return new MqttSource(vertx, config).getSource();
    }

    @Override
    public SubscriberBuilder<? extends Message, Void> getSubscriberBuilder(Config config) {
        return new MqttSink(vertx, config).getSink();
    }
}
