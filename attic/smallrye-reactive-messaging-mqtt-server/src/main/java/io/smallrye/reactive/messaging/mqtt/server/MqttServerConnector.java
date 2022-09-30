package io.smallrye.reactive.messaging.mqtt.server;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(MqttServerConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "auto-generated-client-id", type = "boolean", direction = INCOMING, description = "Set if the MQTT server must generate clientId automatically", defaultValue = "true")
@ConnectorAttribute(name = "ssl", type = "boolean", direction = INCOMING, description = "Set whether SSL/TLS is enabled", defaultValue = "false")
@ConnectorAttribute(name = "max-message-size", type = "int", direction = INCOMING, description = "Set max MQTT message size. -1 means no limit", defaultValue = "-1")
@ConnectorAttribute(name = "timeout-on-connect", type = "int", direction = INCOMING, description = "Connect timeout in seconds", defaultValue = "90")
@ConnectorAttribute(name = "receive-buffer-size", type = "int", direction = INCOMING, description = "The receive buffer size", defaultValue = "-1")
@ConnectorAttribute(name = "host", type = "string", direction = INCOMING, description = "Set the MQTT server host name/IP", defaultValue = "0.0.0.0")
@ConnectorAttribute(name = "port", type = "int", description = "Set the MQTT server port. Default to 8883 if ssl is enabled, or 1883 without ssl", direction = INCOMING)
@ConnectorAttribute(name = "broadcast", description = "Whether or not the messages should be dispatched to multiple consumers", type = "boolean", direction = INCOMING, defaultValue = "false")
public class MqttServerConnector implements IncomingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-mqtt-server";
    private final Vertx vertx;
    private MqttServerSource source = null;

    @Inject
    MqttServerConnector(ExecutionHolder executionHolder) {
        this.vertx = executionHolder.vertx();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (source != null) {
            source.close();
        }
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        if (source == null) {
            source = new MqttServerSource(vertx, new MqttServerConnectorIncomingConfiguration(config));
        }
        return source.source();
    }

    int port() {
        return source == null ? 0 : source.port();
    }
}
