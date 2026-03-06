package io.smallrye.reactive.messaging.rabbitmq.og;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import java.util.List;
import java.util.Map;
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

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.CredentialsProvider;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.core.Vertx;

@ApplicationScoped
@Connector(RabbitMQConnector.CONNECTOR_NAME)

// RabbitMQ Client configuration
@ConnectorAttribute(name = "username", direction = INCOMING_AND_OUTGOING, description = "The username used to authenticate to the broker", type = "string", alias = "rabbitmq-username")
@ConnectorAttribute(name = "password", direction = INCOMING_AND_OUTGOING, description = "The password used to authenticate to the broker", type = "string", alias = "rabbitmq-password")
@ConnectorAttribute(name = "host", direction = INCOMING_AND_OUTGOING, description = "The broker hostname", type = "string", alias = "rabbitmq-host", defaultValue = "localhost")
@ConnectorAttribute(name = "port", direction = INCOMING_AND_OUTGOING, description = "The broker port", type = "int", alias = "rabbitmq-port", defaultValue = "5672")
@ConnectorAttribute(name = "addresses", direction = INCOMING_AND_OUTGOING, description = "The multiple addresses for cluster mode, when given overrides the host and port", type = "string", alias = "rabbitmq-addresses")
@ConnectorAttribute(name = "ssl", direction = INCOMING_AND_OUTGOING, description = "Whether or not the connection should use SSL", type = "boolean", alias = "rabbitmq-ssl", defaultValue = "false")
@ConnectorAttribute(name = "ssl.hostname-verification-algorithm", type = "string", direction = INCOMING_AND_OUTGOING, description = "Set the hostname verifier algorithm for the TLS connection. Accepted values are `HTTPS`, and `NONE` (defaults). `NONE` disables the hostname verification.", defaultValue = "NONE")
@ConnectorAttribute(name = "trust-all", direction = INCOMING_AND_OUTGOING, description = "Whether to skip trust certificate verification", type = "boolean", alias = "rabbitmq-trust-all", defaultValue = "false")
@ConnectorAttribute(name = "trust-store-path", direction = INCOMING_AND_OUTGOING, description = "The path to a JKS trust store", type = "string", alias = "rabbitmq-trust-store-path")
@ConnectorAttribute(name = "trust-store-password", direction = INCOMING_AND_OUTGOING, description = "The password of the JKS trust store", type = "string", alias = "rabbitmq-trust-store-password")
@ConnectorAttribute(name = "connection-timeout", direction = INCOMING_AND_OUTGOING, description = "The TCP connection timeout (ms); 0 is interpreted as no timeout", type = "int", defaultValue = "60000")
@ConnectorAttribute(name = "handshake-timeout", direction = INCOMING_AND_OUTGOING, description = "The AMQP 0-9-1 protocol handshake timeout (ms)", type = "int", defaultValue = "10000")
@ConnectorAttribute(name = "automatic-recovery-enabled", direction = INCOMING_AND_OUTGOING, description = "Whether automatic connection recovery is enabled", type = "boolean", defaultValue = "true")
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
@ConnectorAttribute(name = "client-options-name", direction = INCOMING_AND_OUTGOING, description = "The name of the RabbitMQ ConnectionFactory bean used to customize the RabbitMQ client configuration", type = "string", alias = "rabbitmq-client-options-name")
@ConnectorAttribute(name = "credentials-provider-name", direction = INCOMING_AND_OUTGOING, description = "The name of the RabbitMQ Credentials Provider bean used to provide dynamic credentials to the RabbitMQ client", type = "string", alias = "rabbitmq-credentials-provider-name")

// Health
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "health-readiness-enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether readiness health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "health-lazy-subscription", type = "boolean", direction = INCOMING, description = "Whether the liveness and readiness checks should report 'ok' when there is no subscription yet. This is useful when injecting the channel with `@Inject @Channel(\"...\") Multi<...> multi;`", defaultValue = "false")

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
@ConnectorAttribute(name = "routing-keys", direction = INCOMING, description = "A comma-separated list of routing keys to bind the queue to the exchange. Relevant only if 'exchange.type' is topic or direct", type = "string", defaultValue = "#")
@ConnectorAttribute(name = "arguments", direction = INCOMING, description = "A comma-separated list of arguments [key1:value1,key2:value2,...] to bind the queue to the exchange. Relevant only if 'exchange.type' is headers", type = "string")
@ConnectorAttribute(name = "consumer-arguments", direction = INCOMING, description = "A comma-separated list of arguments [key1:value1,key2:value2,...] for created consumer", type = "string")
@ConnectorAttribute(name = "consumer-tag", direction = INCOMING, description = "The consumer-tag option for created consumer, if not provided the consumer gets assigned a tag generated by the broker", type = "string")
@ConnectorAttribute(name = "consumer-exclusive", direction = INCOMING, description = "The exclusive flag for created consumer", type = "boolean")
@ConnectorAttribute(name = "content-type-override", direction = INCOMING, description = "Override the content_type attribute of the incoming message, should be a valid MINE type", type = "string")
@ConnectorAttribute(name = "max-outstanding-messages", direction = INCOMING, description = "The maximum number of outstanding/unacknowledged messages being processed by the connector at a time; must be a positive number", type = "int")

// Message producer
@ConnectorAttribute(name = "max-inflight-messages", direction = OUTGOING, description = "The maximum number of messages to be written to RabbitMQ concurrently; must be a positive number", type = "long", defaultValue = "1024")
@ConnectorAttribute(name = "default-routing-key", direction = OUTGOING, description = "The default routing key to use when sending messages to the exchange", type = "string", defaultValue = "")
@ConnectorAttribute(name = "default-ttl", direction = OUTGOING, description = "If specified, the time (ms) sent messages can remain in queues undelivered before they are dead", type = "long")
@ConnectorAttribute(name = "publish-confirms", direction = OUTGOING, description = "If set to true, published messages are acknowledged when the publish confirm is received from the broker", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "retry-on-fail-attempts", direction = OUTGOING, description = "The number of tentative to retry on failure", type = "int", defaultValue = "6")
@ConnectorAttribute(name = "retry-on-fail-interval", direction = OUTGOING, description = "The interval (in seconds) between two sending attempts", type = "int", defaultValue = "5")

// Tracing
@ConnectorAttribute(name = "tracing.enabled", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "tracing.attribute-headers", direction = INCOMING_AND_OUTGOING, description = "A comma-separated list of headers that should be recorded as span attributes. Relevant only if tracing.enabled=true", type = "string", defaultValue = "")

public class RabbitMQConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-rabbitmq-og";

    @Inject
    ExecutionHolder executionHolder;

    @Inject
    @Any
    Instance<ConnectionFactory> connectionFactories;

    @Inject
    @Any
    Instance<CredentialsProvider> credentialsProviders;

    @Inject
    @Any
    Instance<Map<String, ?>> configMaps;

    @Inject
    @Any
    Instance<ClientCustomizer<ConnectionFactory>> configCustomizers;

    @Inject
    Instance<OpenTelemetry> openTelemetryInstance;

    private final List<io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel> incomings = new CopyOnWriteArrayList<>();
    private final List<io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel> outgoings = new CopyOnWriteArrayList<>();

    RabbitMQConnector() {
        // used for proxies
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        RabbitMQConnectorIncomingConfiguration ic = new RabbitMQConnectorIncomingConfiguration(config);

        // Create ConnectionFactory
        ConnectionFactory factory = io.smallrye.reactive.messaging.rabbitmq.og.internals.RabbitMQClientHelper
                .createConnectionFactory(ic, connectionFactories, credentialsProviders, configCustomizers);

        // Create ConnectionHolder
        ConnectionHolder holder = new ConnectionHolder(factory, ic.getChannel(),
                executionHolder.vertx(), ic.getReconnectAttempts(), ic.getReconnectInterval());

        // Create IncomingRabbitMQChannel
        io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel channel = new io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel(
                holder, ic, configMaps, openTelemetryInstance);

        incomings.add(channel);

        // Multi already implements Flow.Publisher
        return channel.getStream();
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        RabbitMQConnectorOutgoingConfiguration oc = new RabbitMQConnectorOutgoingConfiguration(config);

        // Create ConnectionFactory
        ConnectionFactory factory = io.smallrye.reactive.messaging.rabbitmq.og.internals.RabbitMQClientHelper
                .createConnectionFactory(oc, connectionFactories, credentialsProviders, configCustomizers);

        // Create ConnectionHolder
        ConnectionHolder holder = new ConnectionHolder(factory, oc.getChannel(),
                executionHolder.vertx(), oc.getReconnectAttempts(), oc.getReconnectInterval());

        // Create OutgoingRabbitMQChannel
        io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel channel = new io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel(
                holder, oc, configMaps, openTelemetryInstance);

        outgoings.add(channel);

        // Wrap Reactive Streams Subscriber as Flow.Subscriber
        return new Flow.Subscriber<Message<?>>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                channel.onSubscribe(new org.reactivestreams.Subscription() {
                    @Override
                    public void request(long n) {
                        subscription.request(n);
                    }

                    @Override
                    public void cancel() {
                        subscription.cancel();
                    }
                });
            }

            @Override
            public void onNext(Message<?> message) {
                channel.onNext(message);
            }

            @Override
            public void onError(Throwable throwable) {
                channel.onError(throwable);
            }

            @Override
            public void onComplete() {
                channel.onComplete();
            }
        };
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();

        // Aggregate readiness from all incoming channels
        for (io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel incoming : incomings) {
            builder = incoming.isReady(builder);
        }

        // Aggregate readiness from all outgoing channels
        for (io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel outgoing : outgoings) {
            builder = outgoing.isReady(builder);
        }

        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();

        // Aggregate liveness from all incoming channels
        for (io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel incoming : incomings) {
            builder = incoming.isAlive(builder);
        }

        // Aggregate liveness from all outgoing channels
        for (io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel outgoing : outgoings) {
            builder = outgoing.isAlive(builder);
        }

        return builder.build();
    }

    public void terminate(
            @SuppressWarnings("unused") @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object ignored) {
        // Clean up all incoming channels
        for (io.smallrye.reactive.messaging.rabbitmq.og.internals.IncomingRabbitMQChannel incoming : incomings) {
            try {
                incoming.cancel();
            } catch (Exception e) {
                // Log but continue cleanup
                e.printStackTrace();
            }
        }
        incomings.clear();

        // Clean up all outgoing channels
        for (io.smallrye.reactive.messaging.rabbitmq.og.internals.OutgoingRabbitMQChannel outgoing : outgoings) {
            try {
                outgoing.onComplete();
            } catch (Exception e) {
                // Log but continue cleanup
                e.printStackTrace();
            }
        }
        outgoings.clear();
    }

    public Vertx vertx() {
        return executionHolder.vertx().getDelegate();
    }

    public Instance<ConnectionFactory> connectionFactories() {
        return connectionFactories;
    }

    public Instance<CredentialsProvider> credentialsProviders() {
        return credentialsProviders;
    }

    public Instance<Map<String, ?>> configMaps() {
        return configMaps;
    }

    public Instance<ClientCustomizer<ConnectionFactory>> configCustomizers() {
        return configCustomizers;
    }
}
