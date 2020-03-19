package io.smallrye.reactive.messaging.http;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(HttpConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "url", type = "string", direction = OUTGOING, description = "The targeted URL", mandatory = true)
@ConnectorAttribute(name = "method", type = "string", direction = OUTGOING, description = " The HTTP method (either `POST` or `PUT`)", defaultValue = "POST")
@ConnectorAttribute(name = "converter", type = "string", direction = OUTGOING, description = "The converter classname used to serialized the outgoing message in the HTTP body")

@ConnectorAttribute(name = "host", type = "string", direction = INCOMING, description = "the host (interface) on which the server is opened", defaultValue = "0.0.0.0")
@ConnectorAttribute(name = "port", type = "int", direction = INCOMING, description = "the port", defaultValue = "8080")
public class HttpConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    public static final String CONNECTOR_NAME = "smallrye-http";

    @Inject
    private ExecutionHolder executionHolder;

    private boolean internalVertxInstance = false;
    private Vertx vertx;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        return new HttpSink(vertx, new HttpConnectorOutgoingConfiguration(config)).sink();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        return new HttpSource(vertx, new HttpConnectorIncomingConfiguration(config)).source();
    }

}
