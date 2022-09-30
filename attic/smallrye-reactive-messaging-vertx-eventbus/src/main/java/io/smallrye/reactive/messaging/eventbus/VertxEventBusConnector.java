package io.smallrye.reactive.messaging.eventbus;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
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
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
public class VertxEventBusConnector implements OutgoingConnectorFactory, IncomingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-vertx-eventbus";

    @Inject
    ExecutionHolder executionHolder;

    private Vertx vertx;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
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
