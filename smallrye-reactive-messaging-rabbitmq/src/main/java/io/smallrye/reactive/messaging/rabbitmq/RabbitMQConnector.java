package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import com.rabbitmq.client.impl.CredentialsProvider;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.internals.IncomingRabbitMQChannel;
import io.smallrye.reactive.messaging.rabbitmq.internals.OutgoingRabbitMQChannel;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

@ApplicationScoped
@Connector(RabbitMQConnector.CONNECTOR_NAME)

// RabbitMQClient configuration
@ConnectorAttribute(name = "username", direction = INCOMING_AND_OUTGOING, description = "The username used to authenticate to the broker", type = "string", alias = "rabbitmq-username")
@ConnectorAttribute(name = "password", direction = INCOMING_AND_OUTGOING, description = "The password used to authenticate to the broker", type = "string", alias = "rabbitmq-password")
@ConnectorAttribute(name = "host", direction = INCOMING_AND_OUTGOING, description = "The broker hostname", type = "string", alias = "rabbitmq-host", defaultValue = "localhost")
@ConnectorAttribute(name = "port", direction = INCOMING_AND_OUTGOING, description = "The broker port", type = "int", alias = "rabbitmq-port", defaultValue = "5672")
@ConnectorAttribute(name = "addresses", direction = INCOMING_AND_OUTGOING, description = "The multiple addresses for cluster mode, when given overrides the host and port", type = "string", alias = "rabbitmq-addresses")
@ConnectorAttribute(name = "ssl", direction = INCOMING_AND_OUTGOING, description = "Whether or not the connection should use SSL", type = "boolean", alias = "rabbitmq-ssl", defaultValue = "false")
@ConnectorAttribute(name = "trust-all", direction = INCOMING_AND_OUTGOING, description = "Whether to skip trust certificate verification", type = "boolean", alias = "rabbitmq-trust-all", defaultValue = "false")
@ConnectorAttribute(name = "trust-store-path", direction = INCOMING_AND_OUTGOING, description = "The path to a JKS trust store", type = "string", alias = "rabbitmq-trust-store-path")
@ConnectorAttribute(name = "trust-store-password", direction = INCOMING_AND_OUTGOING, description = "The password of the JKS trust store", type = "string", alias = "rabbitmq-trust-store-password")
@ConnectorAttribute(name = "connection-timeout", direction = INCOMING_AND_OUTGOING, description = "The TCP connection timeout (ms); 0 is interpreted as no timeout", type = "int", defaultValue = "60000")
@ConnectorAttribute(name = "handshake-timeout", direction = INCOMING_AND_OUTGOING, description = "The AMQP 0-9-1 protocol handshake timeout (ms)", type = "int", defaultValue = "10000")
@ConnectorAttribute(name = "automatic-recovery-enabled", direction = INCOMING_AND_OUTGOING, description = "Whether automatic connection recovery is enabled", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "automatic-recovery-on-initial-connection", direction = INCOMING_AND_OUTGOING, description = "Whether automatic recovery on initial connections is enabled", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "reconnect-attempts", direction = INCOMING_AND_OUTGOING, description = "The number of reconnection attempts", type = "int", alias = "rabbitmq-reconnect-attempts", defaultValue = "100")
@ConnectorAttribute(name = "reconnect-interval", direction = INCOMING_AND_OUTGOING, description = "The interval (in seconds) between two reconnection attempts", type = "int", alias = "rabbitmq-reconnect-interval", defaultValue = "10")
@ConnectorAttribute(name = "network-recovery-interval", direction = INCOMING_AND_OUTGOING, description = "How long (ms) will automatic recovery wait before attempting to reconnect", type = "int", defaultValue = "5000")
@ConnectorAttribute(name = "user", direction = INCOMING_AND_OUTGOING, description = "The user name to use when connecting to the broker", type = "string", defaultValue = "guest")
@ConnectorAttribute(name = "include-properties", direction = INCOMING_AND_OUTGOING, description = "Whether to include properties when a broker message is passed on the event bus", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "requested-channel-max", direction = INCOMING_AND_OUTGOING, description = "The initially requested maximum channel number", type = "int", defaultValue = "2047")
@ConnectorAttribute(name = "requested-heartbeat", direction = INCOMING_AND_OUTGOING, description = "The initially requested heartbeat interval (seconds), zero for none", type = "int", defaultValue = "60")
@ConnectorAttribute(name = "use-nio", direction = INCOMING_AND_OUTGOING, description = "Whether usage of NIO Sockets is enabled", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "virtual-host", direction = INCOMING_AND_OUTGOING, description = "The virtual host to use when connecting to the broker", type = "string", defaultValue = "/", alias = "rabbitmq-virtual-host")
@ConnectorAttribute(name = "client-options-name", direction = INCOMING_AND_OUTGOING, description = "The name of the RabbitMQ Client Option bean used to customize the RabbitMQ client configuration", type = "string", alias = "rabbitmq-client-options-name")
@ConnectorAttribute(name = "credentials-provider-name", direction = INCOMING_AND_OUTGOING, description = "The name of the RabbitMQ Credentials Provider bean used to provide dynamic credentials to the RabbitMQ client", type = "string", alias = "rabbitmq-credentials-provider-name")

// Health
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "health-readiness-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether readiness health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "health-lazy-subscription", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether the liveness and readiness checks should report 'ok' when there is no subscription yet. This is useful when injecting the channel with `@Inject @Channel(\"...\") Multi<...> multi;`", defaultValue = "false")

// Exchange
@ConnectorAttribute(name = "exchange.name", direction = INCOMING_AND_OUTGOING, description = "The exchange that messages are published to or consumed from. If not set, the channel name is used. If set to \"\", the default exchange is used.", type = "string")
@ConnectorAttribute(name = "exchange.durable", direction = INCOMING_AND_OUTGOING, description = "Whether the exchange is durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "exchange.auto-delete", direction = INCOMING_AND_OUTGOING, description = "Whether the exchange should be deleted after use", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "exchange.type", direction = INCOMING_AND_OUTGOING, description = "The exchange type: direct, fanout, headers or topic (default)", type = "string", defaultValue = "topic")
@ConnectorAttribute(name = "exchange.declare", direction = INCOMING_AND_OUTGOING, description = "Whether to declare the exchange; set to false if the exchange is expected to be set up independently", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "exchange.arguments", direction = INCOMING_AND_OUTGOING, description = "The identifier of the key-value Map exposed as bean used to provide arguments for exchange creation", type = "string", defaultValue = "rabbitmq-exchange-arguments")

// Queue
@ConnectorAttribute(name = "queue.name", direction = INCOMING, description = "The queue from which messages are consumed. If not set, the channel name is used.", type = "string")
@ConnectorAttribute(name = "queue.durable", direction = INCOMING, description = "Whether the queue is durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "queue.exclusive", direction = INCOMING, description = "Whether the queue is for exclusive use", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "queue.auto-delete", direction = INCOMING, description = "Whether the queue should be deleted after use", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "queue.declare", direction = INCOMING, description = "Whether to declare the queue and binding; set to false if these are expected to be set up independently", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "queue.ttl", direction = INCOMING, description = "If specified, the time (ms) for which a message can remain in the queue undelivered before it is dead", type = "long")
@ConnectorAttribute(name = "queue.single-active-consumer", direction = INCOMING, description = "If set to true, only one consumer can actively consume messages", type = "boolean")
@ConnectorAttribute(name = "queue.x-queue-type", direction = INCOMING, description = "If automatically declare queue, we can choose different types of queue [quorum, classic, stream]", type = "string")
@ConnectorAttribute(name = "queue.x-queue-mode", direction = INCOMING, description = "If automatically declare queue, we can choose different modes of queue [lazy, default]", type = "string")
@ConnectorAttribute(name = "max-outgoing-internal-queue-size", direction = OUTGOING, description = "The maximum size of the outgoing internal queue", type = "int")
@ConnectorAttribute(name = "max-incoming-internal-queue-size", direction = INCOMING, description = "The maximum size of the incoming internal queue", type = "int", defaultValue = "500000")
@ConnectorAttribute(name = "connection-count", direction = INCOMING, description = "The number of RabbitMQ connections to create for consuming from this queue. This might be necessary to consume from a sharded queue with a single client.", type = "int", defaultValue = "1")
@ConnectorAttribute(name = "queue.x-max-priority", direction = INCOMING, description = "Define priority level queue consumer", type = "int")
@ConnectorAttribute(name = "queue.x-delivery-limit", direction = INCOMING, description = "If queue.x-queue-type is quorum, when a message has been returned more times than the limit the message will be dropped or dead-lettered", type = "long")
@ConnectorAttribute(name = "queue.arguments", direction = INCOMING, description = "The identifier of the key-value Map exposed as bean used to provide arguments for queue creation", type = "string", defaultValue = "rabbitmq-queue-arguments")

// DLQs
@ConnectorAttribute(name = "auto-bind-dlq", direction = INCOMING, description = "Whether to automatically declare the DLQ and bind it to the binder DLX", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "dead-letter-queue-name", direction = INCOMING, description = "The name of the DLQ; if not supplied will default to the queue name with '.dlq' appended", type = "string")
@ConnectorAttribute(name = "dead-letter-exchange", direction = INCOMING, description = "A DLX to assign to the queue. Relevant only if auto-bind-dlq is true", type = "string", defaultValue = "DLX")
@ConnectorAttribute(name = "dead-letter-exchange-type", direction = INCOMING, description = "The type of the DLX to assign to the queue. Relevant only if auto-bind-dlq is true", type = "string", defaultValue = "direct")
@ConnectorAttribute(name = "dead-letter-exchange.arguments", direction = INCOMING, description = "The identifier of the key-value Map exposed as bean used to provide arguments for dead-letter-exchange creation", type = "string")
@ConnectorAttribute(name = "dead-letter-routing-key", direction = INCOMING, description = "A dead letter routing key to assign to the queue; if not supplied will default to the queue name", type = "string")
@ConnectorAttribute(name = "dlx.declare", direction = INCOMING, description = "Whether to declare the dead letter exchange binding. Relevant only if auto-bind-dlq is true; set to false if these are expected to be set up independently", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "dead-letter-queue-type", direction = INCOMING, description = "If automatically declare DLQ, we can choose different types of DLQ [quorum, classic, stream]", type = "string")
@ConnectorAttribute(name = "dead-letter-queue-mode", direction = INCOMING, description = "If automatically declare DLQ, we can choose different modes of DLQ [lazy, default]", type = "string")
@ConnectorAttribute(name = "dead-letter-queue.arguments", direction = INCOMING, description = "The identifier of the key-value Map exposed as bean used to provide arguments for dead-letter-queue creation", type = "string")
@ConnectorAttribute(name = "dead-letter-ttl", direction = INCOMING, description = "If specified, the time (ms) for which a message can remain in DLQ undelivered before it is dead. Relevant only if auto-bind-dlq is true", type = "long")
@ConnectorAttribute(name = "dead-letter-dlx", direction = INCOMING, description = "If specified, a DLX to assign to the DLQ. Relevant only if auto-bind-dlq is true", type = "string")
@ConnectorAttribute(name = "dead-letter-dlx-routing-key", direction = INCOMING, description = "If specified, a dead letter routing key to assign to the DLQ. Relevant only if auto-bind-dlq is true", type = "string")

// Message consumer
@ConnectorAttribute(name = "failure-strategy", direction = INCOMING, description = "The failure strategy to apply when a RabbitMQ message is nacked. Accepted values are `fail`, `accept`, `reject` (default), `requeue` or name of a bean", type = "string", defaultValue = "reject")
@ConnectorAttribute(name = "broadcast", direction = INCOMING, description = "Whether the received RabbitMQ messages must be dispatched to multiple _subscribers_", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "auto-acknowledgement", direction = INCOMING, description = "Whether the received RabbitMQ messages must be acknowledged when received; if true then delivery constitutes acknowledgement", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "keep-most-recent", direction = INCOMING, description = "Whether to discard old messages instead of recent ones", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "routing-keys", direction = INCOMING, description = "A comma-separated list of routing keys to bind the queue to the exchange. Relevant only if 'exchange.type' is topic or direct", type = "string", defaultValue = "#")
@ConnectorAttribute(name = "arguments", direction = INCOMING, description = "A comma-separated list of arguments [key1:value1,key2:value2,...] to bind the queue to the exchange. Relevant only if 'exchange.type' is headers", type = "string")
@ConnectorAttribute(name = "content-type-override", direction = INCOMING, description = "Override the content_type attribute of the incoming message, should be a valid MINE type", type = "string")
@ConnectorAttribute(name = "max-outstanding-messages", direction = INCOMING, description = "The maximum number of outstanding/unacknowledged messages being processed by the connector at a time; must be a positive number", type = "int")

// Message producer
@ConnectorAttribute(name = "max-inflight-messages", direction = OUTGOING, description = "The maximum number of messages to be written to RabbitMQ concurrently; must be a positive number", type = "long", defaultValue = "1024")
@ConnectorAttribute(name = "default-routing-key", direction = OUTGOING, description = "The default routing key to use when sending messages to the exchange", type = "string", defaultValue = "")
@ConnectorAttribute(name = "default-ttl", direction = OUTGOING, description = "If specified, the time (ms) sent messages can remain in queues undelivered before they are dead", type = "long")

// Tracing
@ConnectorAttribute(name = "tracing.enabled", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "tracing.attribute-headers", direction = INCOMING_AND_OUTGOING, description = "A comma-separated list of headers that should be recorded as span attributes. Relevant only if tracing.enabled=true", type = "string", defaultValue = "")

public class RabbitMQConnector implements InboundConnector, OutboundConnector, HealthReporter {
    public static final String CONNECTOR_NAME = "smallrye-rabbitmq";

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    @Any
    Instance<RabbitMQOptions> clientOptions;

    @Inject
    @Any
    Instance<CredentialsProvider> credentialsProviders;

    @Inject
    @Any
    Instance<RabbitMQFailureHandler.Factory> failureHandlerFactories;
    private List<IncomingRabbitMQChannel> incomings = new CopyOnWriteArrayList<>();
    private List<OutgoingRabbitMQChannel> outgoings = new CopyOnWriteArrayList<>();
    private Map<String, RabbitMQClient> clients = new ConcurrentHashMap<>();

    @Inject
    @Any
    Instance<Map<String, ?>> configMaps;

    RabbitMQConnector() {
        // used for proxies
    }

    /**
     * Creates a <em>channel</em> for the given configuration. The channel's configuration is associated with a
     * specific {@code connector}, using the {@link Connector} qualifier's parameter indicating a key to
     * which {@link IncomingConnectorFactory} to use.
     *
     * <p>
     * Note that the connection to the <em>transport</em> or <em>broker</em> is generally postponed until the
     * subscription occurs.
     *
     * @param config the configuration, must not be {@code null}, must contain the {@link #CHANNEL_NAME_ATTRIBUTE}
     *        attribute.
     * @return the created {@link Flow.Publisher}, will not be {@code null}.
     * @throws IllegalArgumentException if the configuration is invalid.
     * @throws NoSuchElementException if the configuration does not contain an expected attribute.
     */
    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(final Config config) {
        final RabbitMQConnectorIncomingConfiguration ic = new RabbitMQConnectorIncomingConfiguration(config);
        IncomingRabbitMQChannel incoming = new IncomingRabbitMQChannel(
                this, ic, failureHandlerFactories, clientOptions, credentialsProviders, configMaps);
        this.incomings.add(incoming);
        return incoming.getStream();
    }

    /**
     * Creates a <em>channel</em> for the given configuration. The channel's configuration is associated with a
     * specific {@code connector}, using the {@link Connector} qualifier's parameter indicating a key to
     * which {@link org.eclipse.microprofile.reactive.messaging.Outgoing} to use.
     * <p>
     * Note that the connection to the <em>transport</em> or <em>broker</em> is generally postponed until the
     * subscription.
     *
     * @param config the configuration, never {@code null}, must contain the {@link #CHANNEL_NAME_ATTRIBUTE}
     *        attribute.
     * @return the created {@link SubscriberBuilder}, must not be {@code null}.
     * @throws IllegalArgumentException if the configuration is invalid.
     * @throws NoSuchElementException if the configuration does not contain an expected attribute.
     */
    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(final Config config) {
        final RabbitMQConnectorOutgoingConfiguration oc = new RabbitMQConnectorOutgoingConfiguration(config);
        OutgoingRabbitMQChannel outgoing = new OutgoingRabbitMQChannel(this, oc, clientOptions, credentialsProviders,
                configMaps);
        outgoings.add(outgoing);
        return outgoing.getSubscriber();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (IncomingRabbitMQChannel incoming : incomings) {
            builder = incoming.isReady(builder);
        }

        for (OutgoingRabbitMQChannel outgoing : outgoings) {
            builder = outgoing.isReady(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (IncomingRabbitMQChannel incoming : incomings) {
            builder = incoming.isAlive(builder);
        }

        for (OutgoingRabbitMQChannel outgoing : outgoings) {
            builder = outgoing.isAlive(builder);
        }
        return builder.build();
    }

    /**
     * Application shutdown tidy up; cancels all subscriptions and stops clients.
     *
     * @param ignored the incoming event, ignored
     */
    public void terminate(
            @SuppressWarnings("unused") @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object ignored) {
        for (IncomingRabbitMQChannel incoming : incomings) {
            incoming.terminate();
        }

        for (OutgoingRabbitMQChannel outgoing : outgoings) {
            outgoing.terminate();
        }

        clients.forEach((channel, rabbitMQClient) -> rabbitMQClient.stopAndAwait());
        clients.clear();
    }

    public Vertx vertx() {
        return executionHolder.vertx();
    }

    public void addClient(String channel, RabbitMQClient client) {
        clients.put(channel, client);
    }

    public void reportIncomingFailure(String channel, Throwable reason) {
        log.failureReported(channel, reason);
        RabbitMQClient client = clients.remove(channel);
        if (client != null) {
            // Called on vertx context, we can't block: stop clients without waiting
            client.stopAndForget();
        }
    }

}
