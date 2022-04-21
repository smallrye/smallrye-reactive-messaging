package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Destroyed;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(MqttConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "client-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the client identifier")
@ConnectorAttribute(name = "auto-generated-client-id", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if the MQTT client must generate clientId automatically", defaultValue = "true")
@ConnectorAttribute(name = "auto-keep-alive", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if the MQTT client must handle `PINGREQ` automatically", defaultValue = "true")
@ConnectorAttribute(name = "ssl", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set whether SSL/TLS is enabled", defaultValue = "false")
@ConnectorAttribute(name = "ssl.keystore.type", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the keystore type [`pkcs12`, `jks`, `pem`]", defaultValue = "pkcs12")
@ConnectorAttribute(name = "ssl.keystore.location", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the keystore location. In case of `pem` type this is the cert path")
@ConnectorAttribute(name = "ssl.keystore.password", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the keystore password. In case of `pem` type this is the key path")
@ConnectorAttribute(name = "ssl.truststore.type", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the truststore type", defaultValue = "pkcs12")
@ConnectorAttribute(name = "ssl.truststore.location", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the truststore location. In case of `pem` type this is the cert path")
@ConnectorAttribute(name = "ssl.truststore.password", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the truststore password. In case of `pem` type this is not necessary")
@ConnectorAttribute(name = "keep-alive-seconds", type = "int", description = "Set the keep alive timeout in seconds", defaultValue = "30", direction = INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "max-inflight-queue", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set max count of unacknowledged messages", defaultValue = "10")
@ConnectorAttribute(name = "auto-clean-session", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set to start with a clean session (`true` by default)", defaultValue = "true")
@ConnectorAttribute(name = "will-flag", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if will information are provided on connection", defaultValue = "false")
@ConnectorAttribute(name = "will-retain", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if the will message must be retained", defaultValue = "false")
@ConnectorAttribute(name = "will-qos", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set the QoS level for the will message", defaultValue = "0")
@ConnectorAttribute(name = "max-message-size", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set max MQTT message size in bytes", defaultValue = "8092")
@ConnectorAttribute(name = "reconnect-interval-seconds", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set the reconnect interval in seconds", defaultValue = "1")
@ConnectorAttribute(name = "username", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the username to connect to the server")
@ConnectorAttribute(name = "password", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the password to connect to the server")
@ConnectorAttribute(name = "connect-timeout-seconds", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set the connect timeout (in seconds)", defaultValue = "60")
@ConnectorAttribute(name = "trust-all", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set whether all server certificates should be trusted", defaultValue = "false")
@ConnectorAttribute(name = "host", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the MQTT server host name/IP", mandatory = true)
@ConnectorAttribute(name = "port", type = "int", description = "Set the MQTT server port. Default to 8883 if ssl is enabled, or 1883 without ssl", direction = INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "server-name", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the SNI server name")
@ConnectorAttribute(name = "topic", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the MQTT topic. If not set, the channel name is used")
@ConnectorAttribute(name = "qos", type = "int", defaultValue = "0", direction = INCOMING_AND_OUTGOING, description = "Set the QoS level when subscribing to the topic or when sending a message")
@ConnectorAttribute(name = "broadcast", description = "Whether or not the messages should be dispatched to multiple consumers", type = "boolean", direction = INCOMING, defaultValue = "false")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = INCOMING, description = "Specify the failure strategy to apply when a message produced from a MQTT message is nacked. Values can be `fail` (default), or `ignore`", defaultValue = "fail")
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "buffer-size", direction = INCOMING, description = "The size buffer of incoming messages waiting to be processed", type = "int", defaultValue = "128")
public class MqttConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    static final String CONNECTOR_NAME = "smallrye-mqtt";

    @Inject
    private ExecutionHolder executionHolder;

    private Vertx vertx;
    private final List<MqttSource> sources = new CopyOnWriteArrayList<>();
    private final List<MqttSink> sinks = new CopyOnWriteArrayList<>();

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        MqttSource source = new MqttSource(vertx, new MqttConnectorIncomingConfiguration(config));
        sources.add(source);
        return source.getSource();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        MqttSink sink = new MqttSink(vertx, new MqttConnectorOutgoingConfiguration(config));
        sinks.add(sink);
        return sink.getSink();
    }

    public boolean isReady() {
        boolean ready = isSourceReady();

        for (MqttSink sink : sinks) {
            ready = ready && sink.isReady();
        }

        return ready;
    }

    public boolean isSourceReady() {
        boolean ready = true;
        for (MqttSource source : sources) {
            ready = ready && source.isReady();
        }
        return ready;
    }

    public void destroy(@Observes @Destroyed(ApplicationScoped.class) final Object context) {
        Clients.clear();
    }
}
