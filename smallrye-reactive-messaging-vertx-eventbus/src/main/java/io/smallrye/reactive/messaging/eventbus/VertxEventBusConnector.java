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

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(VertxEventBusConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "address", type = "string", direction = Direction.INCOMING_AND_OUTGOING, description = "The event bus address", mandatory = true)
@ConnectorAttribute(name = "broadcast", type = "boolean", direction = Direction.INCOMING, description = "Whether to dispatch the messages to multiple consumers", defaultValue = "false")
@ConnectorAttribute(name = "use-reply-as-ack", type = "boolean", direction = Direction.INCOMING, description = "Whether acknowledgement is done by replying to the incoming message with a _dummy_ reply", defaultValue = "false")
@ConnectorAttribute(name = "expect-reply", type = "boolean", direction = Direction.OUTGOING, description = "Whether the outgoing message is expecting a reply. This reply is used as acknowledgement", defaultValue = "false")
@ConnectorAttribute(name = "publish", type = "boolean", direction = Direction.OUTGOING, description = "Whether the to _publish_ the message to multiple Event Bus consumers. You cannot use `publish` in combination with `expect-reply`.", defaultValue = "false")
@ConnectorAttribute(name = "codec", type = "string", direction = Direction.OUTGOING, description = "The name of the codec used to encode the outgoing message. The codec must have been registered.")
@ConnectorAttribute(name = "timeout", type = "long", direction = Direction.OUTGOING, description = "The reply timeout (in ms), -1 to not set a timeout", defaultValue = "-1")
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
        return new EventBusSource(vertx, new VertxEventBusConnectorIncomingConfiguration(config)).source();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        return new EventBusSink(vertx, new VertxEventBusConnectorOutgoingConfiguration(config)).sink();
    }

}
