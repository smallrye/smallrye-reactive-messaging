package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(AmqpConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "username", direction = INCOMING_AND_OUTGOING, description = "The username used to authenticate to the broker", type = "string", alias = "amqp-username")
@ConnectorAttribute(name = "password", direction = INCOMING_AND_OUTGOING, description = "The password used to authenticate to the broker", type = "string", alias = "amqp-password")
@ConnectorAttribute(name = "host", direction = INCOMING_AND_OUTGOING, description = "The broker hostname", type = "string", alias = "amqp-host", defaultValue = "localhost")
@ConnectorAttribute(name = "port", direction = INCOMING_AND_OUTGOING, description = "The broker port", type = "int", alias = "amqp-port", defaultValue = "5672")
@ConnectorAttribute(name = "use-ssl", direction = INCOMING_AND_OUTGOING, description = "Whether the AMQP connection uses SSL/TLS", type = "boolean", alias = "amqp-use-ssl", defaultValue = "false")
@ConnectorAttribute(name = "virtual-host", direction = INCOMING_AND_OUTGOING, description = "If set, configure the hostname value used for the connection AMQP Open frame and TLS SNI server name (if TLS is in use)", type = "string", alias = "amqp-virtual-host")
@ConnectorAttribute(name = "sni-server-name", direction = INCOMING_AND_OUTGOING, description = "If set, explicitly override the hostname to use for the TLS SNI server name", type = "string", alias = "amqp-sni-server-name")
@ConnectorAttribute(name = "reconnect-attempts", direction = INCOMING_AND_OUTGOING, description = "The number of reconnection attempts", type = "int", alias = "amqp-reconnect-attempts", defaultValue = "100")
@ConnectorAttribute(name = "reconnect-interval", direction = INCOMING_AND_OUTGOING, description = "The interval in second between two reconnection attempts", type = "int", alias = "amqp-reconnect-interval", defaultValue = "10")
@ConnectorAttribute(name = "connect-timeout", direction = INCOMING_AND_OUTGOING, description = "The connection timeout in milliseconds", type = "int", alias = "amqp-connect-timeout", defaultValue = "1000")
@ConnectorAttribute(name = "container-id", direction = INCOMING_AND_OUTGOING, description = "The AMQP container id", type = "string")
@ConnectorAttribute(name = "address", direction = INCOMING_AND_OUTGOING, description = "The AMQP address. If not set, the channel name is used", type = "string")
@ConnectorAttribute(name = "link-name", direction = INCOMING_AND_OUTGOING, description = "The name of the link. If not set, the channel name is used.", type = "string")
@ConnectorAttribute(name = "client-options-name", direction = INCOMING_AND_OUTGOING, description = "The name of the AMQP Client Option bean used to customize the AMQP client configuration", type = "string", alias = "amqp-client-options-name")
@ConnectorAttribute(name = "client-ssl-context-name", direction = INCOMING_AND_OUTGOING, description = "The name of an SSLContext bean to use for connecting to AMQP when SSL is used", type = "string", alias = "amqp-client-ssl-context-name", hiddenFromDocumentation = true)
@ConnectorAttribute(name = "tracing-enabled", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "health-timeout", direction = INCOMING_AND_OUTGOING, description = "The max number of seconds to wait to determine if the connection with the broker is still established for the readiness check. After that threshold, the check is considered as failed.", type = "int", defaultValue = "3")
@ConnectorAttribute(name = "cloud-events", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Enables (default) or disables the Cloud Event support. If enabled on an _incoming_ channel, the connector analyzes the incoming records and try to create Cloud Event metadata. If enabled on an _outgoing_, the connector sends the outgoing messages as Cloud Event if the message includes Cloud Event Metadata.", defaultValue = "true")
@ConnectorAttribute(name = "capabilities", type = "string", direction = INCOMING_AND_OUTGOING, description = " A comma-separated list of capabilities proposed by the sender or receiver client.")
@ConnectorAttribute(name = "retry-on-fail-attempts", direction = INCOMING_AND_OUTGOING, description = "The number of tentative to retry on failure", type = "int", defaultValue = "6")
@ConnectorAttribute(name = "retry-on-fail-interval", direction = INCOMING_AND_OUTGOING, description = "The interval (in seconds) between two sending attempts", type = "int", defaultValue = "5")

@ConnectorAttribute(name = "broadcast", direction = INCOMING, description = "Whether the received AMQP messages must be dispatched to multiple _subscribers_", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "durable", direction = INCOMING, description = "Whether AMQP subscription is durable", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "auto-acknowledgement", direction = INCOMING, description = "Whether the received AMQP messages must be acknowledged when received", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = INCOMING, description = "Specify the failure strategy to apply when a message produced from an AMQP message is nacked. Accepted values are `fail` (default), `accept`, `release`, `reject`, `modified-failed`, `modified-failed-undeliverable-here`", defaultValue = "fail")
@ConnectorAttribute(name = "selector", direction = INCOMING, description = "Sets a message selector. This attribute is used to define an `apache.org:selector-filter:string` filter on the source terminus, using SQL-based syntax to request the server filters which messages are delivered to the receiver (if supported by the server in question). Precise functionality supported and syntax needed can vary depending on the server.", type = "string")

@ConnectorAttribute(name = "durable", direction = OUTGOING, description = "Whether sent AMQP messages are marked durable", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "ttl", direction = OUTGOING, description = "The time-to-live of the send AMQP messages. 0 to disable the TTL", type = "long", defaultValue = "0")
@ConnectorAttribute(name = "credit-retrieval-period", direction = OUTGOING, description = "The period (in milliseconds) between two attempts to retrieve the credits granted by the broker. This time is used when the sender run out of credits.", type = "int", defaultValue = "2000")
@ConnectorAttribute(name = "max-inflight-messages", type = "long", direction = OUTGOING, description = "The maximum number of messages to be written to the broker concurrently. The number of sent messages waiting to be acknowledged by the broker are limited by this value and credits granted by the broker. The default value `0` means only credits apply.", defaultValue = "0")
@ConnectorAttribute(name = "use-anonymous-sender", direction = OUTGOING, description = "Whether or not the connector should use an anonymous sender. Default value is `true` if the broker supports it, `false` otherwise. If not supported, it is not possible to dynamically change the destination address.", type = "boolean")
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "cloud-events-source", type = "string", direction = OUTGOING, description = "Configure the default `source` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `source` attribute itself", alias = "cloud-events-default-source")
@ConnectorAttribute(name = "cloud-events-type", type = "string", direction = OUTGOING, description = "Configure the default `type` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `type` attribute itself", alias = "cloud-events-default-type")
@ConnectorAttribute(name = "cloud-events-subject", type = "string", direction = OUTGOING, description = "Configure the default `subject` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `subject` attribute itself", alias = "cloud-events-default-subject")
@ConnectorAttribute(name = "cloud-events-data-content-type", type = "string", direction = OUTGOING, description = "Configure the default `datacontenttype` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `datacontenttype` attribute itself", alias = "cloud-events-default-data-content-type")
@ConnectorAttribute(name = "cloud-events-data-schema", type = "string", direction = OUTGOING, description = "Configure the default `dataschema` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `dataschema` attribute itself", alias = "cloud-events-default-data-schema")
@ConnectorAttribute(name = "cloud-events-insert-timestamp", type = "boolean", direction = OUTGOING, description = "Whether or not the connector should insert automatically the `time` attribute into the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `time` attribute itself", alias = "cloud-events-default-timestamp", defaultValue = "true")
@ConnectorAttribute(name = "cloud-events-mode", type = "string", direction = OUTGOING, description = "The Cloud Event mode (`structured` or `binary` (default)). Indicates how are written the cloud events in the outgoing record", defaultValue = "binary")
@ConnectorAttribute(name = "lazy-client", type = "boolean", direction = OUTGOING, description = "Whether to create the connection and sender at startup or at first send request", defaultValue = "true")

public class AmqpConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-amqp";

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    @Any
    private Instance<AmqpClientOptions> clientOptions;

    @Inject
    @Any
    private Instance<ClientCustomizer<AmqpClientOptions>> configCustomizers;

    @Inject
    private Instance<OpenTelemetry> openTelemetryInstance;

    private final List<AmqpClient> clients = new CopyOnWriteArrayList<>();

    /**
     * Tracks the consumer connection holder.
     * This map is used for cleanup and health checks.
     */
    private final Map<String, IncomingAmqpChannel> incomingChannels = new ConcurrentHashMap<>();

    /**
     * Tracks the outgoing channels.
     * This map is used for cleanup and health checks.
     */
    private final Map<String, OutgoingAmqpChannel> outgoingChannels = new ConcurrentHashMap<>();

    void setup(ExecutionHolder executionHolder) {
        this.executionHolder = executionHolder;
    }

    AmqpConnector() {
        // used for proxies
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        AmqpConnectorIncomingConfiguration ic = new AmqpConnectorIncomingConfiguration(config);
        AmqpClient client = AmqpClientHelper.createClient(executionHolder.vertx(), ic, clientOptions, configCustomizers);
        addClient(client);

        IncomingAmqpChannel incoming = new IncomingAmqpChannel(ic, client, executionHolder.vertx(),
                openTelemetryInstance, this::reportFailure);
        incomingChannels.put(ic.getChannel(), incoming);

        return incoming.getPublisher();
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        AmqpConnectorOutgoingConfiguration oc = new AmqpConnectorOutgoingConfiguration(config);
        AmqpClient client = AmqpClientHelper.createClient(executionHolder.vertx(), oc, clientOptions, configCustomizers);
        addClient(client);
        OutgoingAmqpChannel outgoing = new OutgoingAmqpChannel(oc, client, executionHolder.vertx(),
                openTelemetryInstance, this::reportFailure);
        outgoingChannels.put(oc.getChannel(), outgoing);
        return outgoing.getSubscriber();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        outgoingChannels.values().forEach(OutgoingAmqpChannel::close);
        incomingChannels.values().forEach(IncomingAmqpChannel::close);
        clients.forEach(c -> {
            // We cannot use andForget as it could report an error is the broker is not available.
            //noinspection ResultOfMethodCallIgnored
            c.close().subscribeAsCompletionStage();
        });
        clients.clear();
    }

    public Vertx getVertx() {
        return executionHolder.vertx();
    }

    public void addClient(AmqpClient client) {
        clients.add(client);
    }

    public List<AmqpClient> getClients() {
        return clients;
    }

    /**
     * Readiness verify that we have an established connection with the broker.
     * If the connection is disconnected, readiness is set to false.
     * However, liveness may still be true because of the retry.
     *
     * @return the report
     */
    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (Map.Entry<String, IncomingAmqpChannel> holder : incomingChannels.entrySet()) {
            try {
                builder.add(holder.getKey(), holder.getValue().isConnected().await()
                        .atMost(Duration.ofSeconds(holder.getValue().getHealthTimeout())));
            } catch (Exception e) {
                builder.add(holder.getKey(), false, e.getMessage());
            }
        }

        for (Map.Entry<String, OutgoingAmqpChannel> sender : outgoingChannels.entrySet()) {
            try {
                builder.add(sender.getKey(), sender.getValue().isConnected().await()
                        .atMost(Duration.ofSeconds(sender.getValue().getHealthTimeout())));
            } catch (Exception e) {
                builder.add(sender.getKey(), false, e.getMessage());
            }
        }

        return builder.build();
    }

    /**
     * Liveness checks if a connection is established with the broker.
     * Liveness is set to false after all the retry attempt have been re-attempted.
     *
     * @return the report
     */
    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (Map.Entry<String, IncomingAmqpChannel> entry : incomingChannels.entrySet()) {
            builder.add(entry.getKey(), entry.getValue().isOpen());
        }
        for (Map.Entry<String, OutgoingAmqpChannel> entry : outgoingChannels.entrySet()) {
            builder.add(entry.getKey(), entry.getValue().isOpen());
        }
        return builder.build();
    }

    @Override
    public HealthReport getStartup() {
        return getLiveness();
    }

    public void reportFailure(String channel, Throwable reason) {
        log.failureReported(channel, reason);
        terminate(null);
    }

}
