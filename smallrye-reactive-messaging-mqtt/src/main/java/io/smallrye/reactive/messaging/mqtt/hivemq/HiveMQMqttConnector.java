package io.smallrye.reactive.messaging.mqtt.hivemq;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Destroyed;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(HiveMQMqttConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "client-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the client identifier")
@ConnectorAttribute(name = "auto-generated-client-id", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if the MQTT client must generate clientId automatically", defaultValue = "true")
@ConnectorAttribute(name = "auto-keep-alive", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if the MQTT client must handle `PINGREQ` automatically", defaultValue = "true")
@ConnectorAttribute(name = "ssl", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set whether SSL/TLS is enabled", defaultValue = "false")
@ConnectorAttribute(name = "keep-alive-seconds", type = "int", description = "Set the keep alive timeout in seconds", defaultValue = "30", direction = INCOMING_AND_OUTGOING)
@ConnectorAttribute(name = "max-inflight-queue", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set max count of unacknowledged messages", defaultValue = "10")
@ConnectorAttribute(name = "auto-clean-session", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set to start with a clean session (`true` by default)", defaultValue = "true")
@ConnectorAttribute(name = "will-flag", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if will information are provided on connection", defaultValue = "false")
@ConnectorAttribute(name = "will-retain", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Set if the will message must be retained", defaultValue = "false")
@ConnectorAttribute(name = "will-qos", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set the QoS level for the will message", defaultValue = "0")
@ConnectorAttribute(name = "max-message-size", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set max MQTT message size in bytes", defaultValue = "8092")
@ConnectorAttribute(name = "reconnect-attempts", type = "int", direction = INCOMING_AND_OUTGOING, description = "Set the max reconnect attempts", defaultValue = "5")
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
@ConnectorAttribute(name = "ca-cart-file", direction = INCOMING_AND_OUTGOING, description = "File containing the self-signed CA for SSL connection", type = "string")
@ConnectorAttribute(name = "check-topic-enabled", direction = INCOMING_AND_OUTGOING, description = "Enable check for liveness/readiness", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "check-topic-name", direction = INCOMING_AND_OUTGOING, description = "Topic Used to check liveness/readiness", type = "string", defaultValue = "$SYS/broker/uptime")
@ConnectorAttribute(name = "readiness-timeout", direction = INCOMING_AND_OUTGOING, description = "Timeout to declare the MQTT Client not ready", type = "int", defaultValue = "20000")
@ConnectorAttribute(name = "liveness-timeout", direction = INCOMING_AND_OUTGOING, description = "Timeout to declare the MQTT Client not alive", type = "int", defaultValue = "120000")
public class HiveMQMqttConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-mqtt-hivemq";

    @Inject
    private ExecutionHolder executionHolder;

    private Vertx vertx;
    private final List<HiveMQMqttSource> sources = new CopyOnWriteArrayList<>();
    private final List<HiveMQMqttSink> sinks = new CopyOnWriteArrayList<>();

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        HiveMQMqttSource source = new HiveMQMqttSource(new HiveMQMqttConnectorIncomingConfiguration(config));
        sources.add(source);
        return source.getSource();
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        HiveMQMqttSink sink = new HiveMQMqttSink(vertx, new HiveMQMqttConnectorOutgoingConfiguration(config));
        sinks.add(sink);
        return sink.getSink();
    }

    public boolean isReady() {
        boolean ready = isSourceReady();

        for (HiveMQMqttSink sink : sinks) {
            ready = ready && sink.isReady();
        }

        return ready;
    }

    public boolean isSourceReady() {
        boolean ready = true;
        for (HiveMQMqttSource source : sources) {
            ready = ready && source.isSubscribed();
        }
        return ready;
    }

    public void destroy(@Observes @Destroyed(ApplicationScoped.class) final Object context) {
        HiveMQClients.clear();
    }

    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();

        HiveMQClients.checkReadiness(builder);

        return builder.build();
    }

    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();

        HiveMQClients.checkLiveness(builder);

        return builder.build();
    }

}
