package io.smallrye.reactive.messaging.rabbitmq;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQExceptions.ex;
import static io.smallrye.reactive.messaging.rabbitmq.i18n.RabbitMQLogging.log;
import static java.time.Duration.ofSeconds;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAck;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAckHandler;
import io.smallrye.reactive.messaging.rabbitmq.ack.RabbitMQAutoAck;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQAccept;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailStop;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQFailureHandler;
import io.smallrye.reactive.messaging.rabbitmq.fault.RabbitMQReject;
import io.smallrye.reactive.messaging.rabbitmq.tracing.TracingUtils;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.rabbitmq.RabbitMQClient;
import io.vertx.mutiny.rabbitmq.RabbitMQConsumer;
import io.vertx.mutiny.rabbitmq.RabbitMQPublisher;
import io.vertx.rabbitmq.QueueOptions;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.rabbitmq.RabbitMQPublisherOptions;

@ApplicationScoped
@Connector(RabbitMQConnector.CONNECTOR_NAME)

// RabbitMQClient configuration
@ConnectorAttribute(name = "username", direction = INCOMING_AND_OUTGOING, description = "The username used to authenticate to the broker", type = "string", alias = "rabbitmq-username")
@ConnectorAttribute(name = "password", direction = INCOMING_AND_OUTGOING, description = "The password used to authenticate to the broker", type = "string", alias = "rabbitmq-password")
@ConnectorAttribute(name = "host", direction = INCOMING_AND_OUTGOING, description = "The broker hostname", type = "string", alias = "rabbitmq-host", defaultValue = "localhost")
@ConnectorAttribute(name = "port", direction = INCOMING_AND_OUTGOING, description = "The broker port", type = "int", alias = "rabbitmq-port", defaultValue = "5672")
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
@ConnectorAttribute(name = "user", direction = INCOMING_AND_OUTGOING, description = "The AMQP user name to use when connecting to the broker", type = "string", defaultValue = "guest")
@ConnectorAttribute(name = "include-properties", direction = INCOMING_AND_OUTGOING, description = "Whether to include properties when a broker message is passed on the event bus", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "requested-channel-max", direction = INCOMING_AND_OUTGOING, description = "The initially requested maximum channel number", type = "int", defaultValue = "2047")
@ConnectorAttribute(name = "requested-heartbeat", direction = INCOMING_AND_OUTGOING, description = "The initially requested heartbeat interval (seconds), zero for none", type = "int", defaultValue = "60")
@ConnectorAttribute(name = "use-nio", direction = INCOMING_AND_OUTGOING, description = "Whether usage of NIO Sockets is enabled", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "virtual-host", direction = INCOMING_AND_OUTGOING, description = "The virtual host to use when connecting to the broker", type = "string", defaultValue = "/", alias = "rabbitmq-virtual-host")

// Exchange
@ConnectorAttribute(name = "exchange.name", direction = INCOMING_AND_OUTGOING, description = "The exchange that messages are published to or consumed from. If not set, the channel name is used", type = "string")
@ConnectorAttribute(name = "exchange.durable", direction = INCOMING_AND_OUTGOING, description = "Whether the exchange is durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "exchange.auto-delete", direction = INCOMING_AND_OUTGOING, description = "Whether the exchange should be deleted after use", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "exchange.type", direction = INCOMING_AND_OUTGOING, description = "The exchange type: direct, fanout, headers or topic (default)", type = "string", defaultValue = "topic")
@ConnectorAttribute(name = "exchange.declare", direction = INCOMING_AND_OUTGOING, description = "Whether to declare the exchange; set to false if the exchange is expected to be set up independently", type = "boolean", defaultValue = "true")

// Queue
@ConnectorAttribute(name = "queue.name", direction = INCOMING, description = "The queue from which messages are consumed.", type = "string", mandatory = true)
@ConnectorAttribute(name = "queue.durable", direction = INCOMING, description = "Whether the queue is durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "queue.exclusive", direction = INCOMING, description = "Whether the queue is for exclusive use", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "queue.auto-delete", direction = INCOMING, description = "Whether the queue should be deleted after use", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "queue.declare", direction = INCOMING, description = "Whether to declare the queue and binding; set to false if these are expected to be set up independently", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "queue.ttl", direction = INCOMING, description = "If specified, the time (ms) for which a message can remain in the queue undelivered before it is dead", type = "long")
@ConnectorAttribute(name = "max-outgoing-internal-queue-size", direction = OUTGOING, description = "The maximum size of the outgoing internal queue", type = "int")
@ConnectorAttribute(name = "max-incoming-internal-queue-size", direction = INCOMING, description = "The maximum size of the incoming internal queue", type = "int")

// DLQs
@ConnectorAttribute(name = "auto-bind-dlq", direction = INCOMING, description = "Whether to automatically declare the DLQ and bind it to the binder DLX", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "dead-letter-queue-name", direction = INCOMING, description = "The name of the DLQ; if not supplied will default to the queue name with '.dlq' appended", type = "string")
@ConnectorAttribute(name = "dead-letter-exchange", direction = INCOMING, description = "A DLX to assign to the queue. Relevant only if auto-bind-dlq is true", type = "string", defaultValue = "DLX")
@ConnectorAttribute(name = "dead-letter-exchange-type", direction = INCOMING, description = "The type of the DLX to assign to the queue. Relevant only if auto-bind-dlq is true", type = "string", defaultValue = "direct")
@ConnectorAttribute(name = "dead-letter-routing-key", direction = INCOMING, description = "A dead letter routing key to assign to the queue; if not supplied will default to the queue name", type = "string")
@ConnectorAttribute(name = "dlx.declare", direction = INCOMING, description = "Whether to declare the dead letter exchange binding. Relevant only if auto-bind-dlq is true; set to false if these are expected to be set up independently", type = "boolean", defaultValue = "false")

// Message consumer
@ConnectorAttribute(name = "failure-strategy", direction = INCOMING, description = "The failure strategy to apply when a RabbitMQ message is nacked. Accepted values are `fail`, `accept`, `reject` (default)", type = "string", defaultValue = "reject")
@ConnectorAttribute(name = "broadcast", direction = INCOMING, description = "Whether the received RabbitMQ messages must be dispatched to multiple _subscribers_", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "auto-acknowledgement", direction = INCOMING, description = "Whether the received RabbitMQ messages must be acknowledged when received; if true then delivery constitutes acknowledgement", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "keep-most-recent", direction = INCOMING, description = "Whether to discard old messages instead of recent ones", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "routing-keys", direction = INCOMING, description = "A comma-separated list of routing keys to bind the queue to the exchange", type = "string", defaultValue = "#")

// Message producer
@ConnectorAttribute(name = "max-inflight-messages", direction = OUTGOING, description = "The maximum number of messages to be written to RabbitMQ concurrently; must be a positive number", type = "long", defaultValue = "1024")
@ConnectorAttribute(name = "default-routing-key", direction = OUTGOING, description = "The default routing key to use when sending messages to the exchange", type = "string", defaultValue = "")
@ConnectorAttribute(name = "default-ttl", direction = OUTGOING, description = "If specified, the time (ms) sent messages can remain in queues undelivered before they are dead", type = "long")

// Tracing
@ConnectorAttribute(name = "tracing.enabled", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "tracing.attribute-headers", direction = INCOMING_AND_OUTGOING, description = "A comma-separated list of headers that should be recorded as span attributes. Relevant only if tracing.enabled=true", type = "string", defaultValue = "")

public class RabbitMQConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, HealthReporter {
    static final String CONNECTOR_NAME = "smallrye-rabbitmq";

    private enum ChannelStatus {
        CONNECTED,
        NOT_CONNECTED,
        INITIALISING
    }

    // The list of RabbitMQClient's currently managed by this connector
    private final List<RabbitMQClient> clients = new CopyOnWriteArrayList<>();

    // Keyed on channel name, value is the channel connection state
    private final Map<String, ChannelStatus> incomingChannelStatus = new ConcurrentHashMap<>();
    private final Map<String, ChannelStatus> outgoingChannelStatus = new ConcurrentHashMap<>();

    // The list of RabbitMQMessageSender's currently managed by this connector
    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    @Inject
    ExecutionHolder executionHolder;

    RabbitMQConnector() {
        // used for proxies
    }

    private static String getExchangeName(final RabbitMQConnectorCommonConfiguration config) {
        return config.getExchangeName().orElse(config.getChannel());
    }

    @PostConstruct
    void init() {
        TracingUtils.initialise();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Multi<? extends Message<?>> getStreamOfMessages(RabbitMQConsumer receiver,
            ConnectionHolder holder,
            RabbitMQConnectorIncomingConfiguration ic,
            RabbitMQFailureHandler onNack,
            RabbitMQAckHandler onAck) {
        final String queueName = ic.getQueueName();
        final boolean isTracingEnabled = ic.getTracingEnabled();
        final List<String> attributeHeaders = Arrays.stream(ic.getTracingAttributeHeaders().split(","))
                .map(String::trim).collect(Collectors.toList());
        log.receiverListeningAddress(queueName);

        // The processor is used to inject AMQP Connection failure in the stream and trigger a retry.
        BroadcastProcessor processor = BroadcastProcessor.create();
        receiver.exceptionHandler(t -> {
            log.receiverError(t);
            processor.onError(t);
        });
        holder.onFailure(processor::onError);

        return Multi.createFrom().deferred(
                () -> {
                    Multi<? extends Message<?>> stream = receiver.toMulti()
                            .map(m -> new IncomingRabbitMQMessage<>(m, holder, isTracingEnabled, onNack, onAck))
                            .map(m -> isTracingEnabled ? TracingUtils.addIncomingTrace(m, queueName, attributeHeaders) : m);
                    return Multi.createBy().merging().streams(stream, processor);
                });
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
     * @return the created {@link PublisherBuilder}, will not be {@code null}.
     * @throws IllegalArgumentException if the configuration is invalid.
     * @throws NoSuchElementException if the configuration does not contain an expected attribute.
     */
    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(final Config config) {
        final RabbitMQConnectorIncomingConfiguration ic = new RabbitMQConnectorIncomingConfiguration(config);
        incomingChannelStatus.put(ic.getChannel(), ChannelStatus.INITIALISING);

        // Create a client
        final RabbitMQClient client = createClient(new RabbitMQConnectorCommonConfiguration(config));

        final ConnectionHolder holder = new ConnectionHolder(client, ic, getVertx());
        final RabbitMQFailureHandler onNack = createFailureHandler(ic);
        final RabbitMQAckHandler onAck = createAckHandler(ic);

        // Ensure we set the queue up
        Uni<RabbitMQClient> uniQueue = holder.getOrEstablishConnection()
                // Once connected, ensure we create the queue from which messages are to be read
                .onItem().call(connection -> establishQueue(connection, ic))
                // If directed to do so, create a DLQ
                .onItem().call(connection -> establishDLQ(connection, ic))
                .onItem().invoke(connection -> incomingChannelStatus.put(ic.getChannel(), ChannelStatus.CONNECTED));

        // Once the queue is set up, set yp a consumer
        final Integer interval = ic.getReconnectInterval();
        final Integer attempts = ic.getReconnectAttempts();
        Multi<? extends Message<?>> multi = uniQueue
                .onItem().transformToUni(connection -> client.basicConsumer(ic.getQueueName(), new QueueOptions()
                        .setAutoAck(ic.getAutoAcknowledgement())
                        .setMaxInternalQueueSize(ic.getMaxIncomingInternalQueueSize().orElse(Integer.MAX_VALUE))
                        .setKeepMostRecent(ic.getKeepMostRecent())))
                .onItem().transformToMulti(consumer -> getStreamOfMessages(consumer, holder, ic, onNack, onAck))
                .plug(m -> {
                    if (attempts > 0) {
                        return m
                                // Retry on failure.
                                .onFailure().invoke(log::retrieveMessagesRetrying)
                                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(interval)).atMost(attempts)
                                .onFailure().invoke(t -> {
                                    incomingChannelStatus.put(ic.getChannel(), ChannelStatus.NOT_CONNECTED);
                                    log.retrieveMessagesNoMoreRetrying(t);
                                });
                    }
                    return m;
                });

        if (Boolean.TRUE.equals(ic.getBroadcast())) {
            multi = multi.broadcast().toAllSubscribers();
        }

        return ReactiveStreams.fromPublisher(multi);
    }

    /**
     * Establish a DLQ, possibly establishing a DLX too
     *
     * @param client the {@link RabbitMQClient}
     * @param ic the {@link RabbitMQConnectorIncomingConfiguration}
     * @return a {@link Uni<String>} containing the DLQ name
     */
    private Uni<?> establishDLQ(final RabbitMQClient client, final RabbitMQConnectorIncomingConfiguration ic) {
        final String deadLetterQueueName = ic.getDeadLetterQueueName().orElse(String.format("%s.dlq", ic.getQueueName()));
        final String deadLetterExchangeName = ic.getDeadLetterExchange();
        final String deadLetterRoutingKey = ic.getDeadLetterRoutingKey().orElse(ic.getQueueName());

        // Declare the exchange if we have been asked to do so
        final Uni<String> dlxFlow = Uni.createFrom()
                .item(() -> ic.getAutoBindDlq() && ic.getDlxDeclare() ? null : deadLetterExchangeName)
                .onItem().ifNull().switchTo(() -> client.exchangeDeclare(deadLetterExchangeName, ic.getDeadLetterExchangeType(),
                        true, false)
                        .onItem().invoke(() -> log.dlxEstablished(deadLetterExchangeName))
                        .onFailure().invoke(ex -> log.unableToEstablishDlx(deadLetterExchangeName, ex))
                        .onItem().transform(v -> deadLetterExchangeName));

        // Declare the queue (and its binding to the exchange) if we have been asked to do so
        return dlxFlow
                .onItem().transform(v -> Boolean.TRUE.equals(ic.getAutoBindDlq()) ? null : deadLetterQueueName)
                .onItem().ifNull().switchTo(
                        () -> client
                                .queueDeclare(deadLetterQueueName, true, false, false)
                                .onItem().invoke(() -> log.queueEstablished(deadLetterQueueName))
                                .onFailure().invoke(ex -> log.unableToEstablishQueue(deadLetterQueueName, ex))
                                .onItem()
                                .call(v -> client.queueBind(deadLetterQueueName, deadLetterExchangeName, deadLetterRoutingKey))
                                .onItem()
                                .invoke(() -> log.bindingEstablished(deadLetterQueueName, deadLetterExchangeName,
                                        deadLetterRoutingKey))
                                .onFailure()
                                .invoke(ex -> log.unableToEstablishBinding(deadLetterQueueName, deadLetterExchangeName, ex))
                                .onItem().transform(v -> deadLetterQueueName));
    }

    private RabbitMQClient createClient(final RabbitMQConnectorCommonConfiguration config) {
        final RabbitMQOptions rabbitMQClientConfig = new RabbitMQOptions()
                .setHost(config.getHost())
                .setPort(config.getPort())
                .setSsl(config.getSsl())
                .setTrustAll(config.getTrustAll())
                .setAutomaticRecoveryEnabled(config.getAutomaticRecoveryEnabled())
                .setAutomaticRecoveryOnInitialConnection(config.getAutomaticRecoveryOnInitialConnection())
                .setReconnectAttempts(config.getReconnectAttempts())
                .setReconnectInterval(config.getReconnectInterval())
                .setUser(config.getUser())
                .setConnectionTimeout(config.getConnectionTimeout())
                .setHandshakeTimeout(config.getHandshakeTimeout())
                .setIncludeProperties(config.getIncludeProperties())
                .setNetworkRecoveryInterval(config.getNetworkRecoveryInterval())
                .setRequestedChannelMax(config.getRequestedChannelMax())
                .setRequestedHeartbeat(config.getRequestedHeartbeat())
                .setUseNio(config.getUseNio())
                .setVirtualHost(config.getVirtualHost());

        // JKS TrustStore
        Optional<String> trustStorePath = config.getTrustStorePath();
        if (trustStorePath.isPresent()) {
            JksOptions jks = new JksOptions();
            jks.setPath(trustStorePath.get());
            config.getTrustStorePassword().ifPresent(jks::setPassword);
            rabbitMQClientConfig.setTrustStoreOptions(jks);
        }

        config.getUsername().ifPresent(rabbitMQClientConfig::setUser);
        config.getPassword().ifPresent(rabbitMQClientConfig::setPassword);

        final RabbitMQClient client = RabbitMQClient.create(getVertx(), rabbitMQClientConfig);
        addClient(client);
        return client;
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
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(final Config config) {
        final RabbitMQConnectorOutgoingConfiguration oc = new RabbitMQConnectorOutgoingConfiguration(config);
        outgoingChannelStatus.put(oc.getChannel(), ChannelStatus.INITIALISING);

        // Create a client
        final RabbitMQClient client = createClient(new RabbitMQConnectorCommonConfiguration(config));

        // This will hold our publisher, assuming we can get hold of one
        final AtomicReference<RabbitMQPublisher> sender = new AtomicReference<>();

        final ConnectionHolder holder = new ConnectionHolder(client, oc, getVertx());
        final Uni<RabbitMQPublisher> getSender = Uni.createFrom().item(sender.get())
                .onItem().ifNull().switchTo(() -> {

                    // If we already have a sender, use it.
                    RabbitMQPublisher current = sender.get();
                    if (current != null && client.isConnected()) {
                        return Uni.createFrom().item(current);
                    }

                    return holder.getOrEstablishConnection()
                            // Once connected, ensure we create the exchange to which messages are to be sent
                            .onItem().call(connection -> establishExchange(connection, oc))
                            // Once exchange exists, create ourselves a publisher
                            .onItem()
                            .transformToUni(connection -> Uni.createFrom().item(RabbitMQPublisher.create(getVertx(), connection,
                                    new RabbitMQPublisherOptions()
                                            .setReconnectAttempts(oc.getReconnectAttempts())
                                            .setReconnectInterval(oc.getReconnectInterval())
                                            .setMaxInternalQueueSize(
                                                    oc.getMaxOutgoingInternalQueueSize().orElse(Integer.MAX_VALUE)))))
                            // Start the publisher
                            .onItem().call(RabbitMQPublisher::start)
                            .invoke(s -> {
                                // Make a note of the publisher and add the channel in the opened state
                                sender.set(s);
                                outgoingChannelStatus.put(oc.getChannel(), ChannelStatus.CONNECTED);
                            });
                })
                // If the downstream cancels or on failure, drop the sender.
                .onFailure().invoke(t -> {
                    sender.set(null);
                    outgoingChannelStatus.put(oc.getChannel(), ChannelStatus.NOT_CONNECTED);
                })
                .onCancellation().invoke(() -> {
                    sender.set(null);
                    outgoingChannelStatus.put(oc.getChannel(), ChannelStatus.NOT_CONNECTED);
                });

        // Set up a sender based on the publisher we established above
        final RabbitMQMessageSender processor = new RabbitMQMessageSender(
                oc,
                getSender);
        subscriptions.add(processor);

        // Return a SubscriberBuilder
        return ReactiveStreams.<Message<?>> builder()
                .via(processor)
                .onError(t -> {
                    log.error(oc.getChannel(), t);
                    outgoingChannelStatus.put(oc.getChannel(), ChannelStatus.NOT_CONNECTED);
                })
                .ignore();
    }

    @Override
    public HealthReport getReadiness() {
        return getHealth(false);
    }

    @Override
    public HealthReport getLiveness() {
        return getHealth(false);
    }

    public HealthReport getHealth(boolean strict) {
        final HealthReport.HealthReportBuilder builder = HealthReport.builder();

        // Add health for incoming channels; since connections are made immediately
        // for subscribers, we insist on channel status being connected
        incomingChannelStatus.forEach((channel, status) -> builder.add(channel, status == ChannelStatus.CONNECTED));

        // Add health for outgoing channels; since connections are made only on
        // first message dispatch for publishers, we allow both connected and initialising
        // unless in strict mode, in order to avoid a possible deadly embrace in
        // kubernetes (k8s will refuse to allow the pod to accept traffic unless it is ready,
        // and only if it is ready can e.g. calls be made to it that would trigger a message dispatch).
        outgoingChannelStatus.forEach((channel, status) -> builder.add(channel,
                (strict) ? status == ChannelStatus.CONNECTED : status != ChannelStatus.NOT_CONNECTED));

        return builder.build();
    }

    /**
     * Application shutdown tidy up; cancels all subscriptions and stops clients.
     *
     * @param ignored the incoming event, ignored
     */
    public void terminate(
            @SuppressWarnings("unused") @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object ignored) {
        subscriptions.forEach(Subscription::cancel);
        clients.forEach(RabbitMQClient::stopAndAwait);
        clients.clear();
    }

    private Vertx getVertx() {
        return executionHolder.vertx();
    }

    private void addClient(RabbitMQClient client) {
        clients.add(client);
    }

    /**
     * Uses a {@link RabbitMQClient} to ensure the required exchange is created.
     *
     * @param client the RabbitMQ client
     * @param config the channel configuration
     * @return a {@link Uni<String>} which yields the exchange name
     */
    private Uni<String> establishExchange(
            final RabbitMQClient client,
            final RabbitMQConnectorCommonConfiguration config) {
        final String exchangeName = getExchangeName(config);

        // Declare the exchange if we have been asked to do so
        return Uni.createFrom().item(() -> Boolean.TRUE.equals(config.getExchangeDeclare()) ? null : exchangeName)
                .onItem().ifNull().switchTo(() -> client.exchangeDeclare(exchangeName, config.getExchangeType(),
                        config.getExchangeDurable(), config.getExchangeAutoDelete())
                        .onItem().invoke(() -> log.exchangeEstablished(exchangeName))
                        .onFailure().invoke(ex -> log.unableToEstablishExchange(exchangeName, ex))
                        .onItem().transform(v -> exchangeName));
    }

    /**
     * Uses a {@link RabbitMQClient} to ensure the required queue-exchange bindings are created.
     *
     * @param client the RabbitMQ client
     * @param ic the incoming channel configuration
     * @return a {@link Uni<String>} which yields the queue name
     */
    private Uni<String> establishQueue(
            final RabbitMQClient client,
            final RabbitMQConnectorIncomingConfiguration ic) {
        final String queueName = ic.getQueueName();

        // Declare the queue (and its binding(s) to the exchange, and TTL) if we have been asked to do so
        final JsonObject queueArgs = new JsonObject();
        queueArgs.put("x-dead-letter-exchange", ic.getDeadLetterExchange());
        queueArgs.put("x-dead-letter-routing-key", ic.getDeadLetterRoutingKey().orElse(queueName));

        ic.getQueueTtl().ifPresent(queueTtl -> {
            if (queueTtl >= 0) {
                queueArgs.put("x-message-ttl", queueTtl);
            } else {
                throw ex.illegalArgumentInvalidQueueTtl();
            }
        });

        return establishExchange(client, ic)
                .onItem().transform(v -> Boolean.TRUE.equals(ic.getQueueDeclare()) ? null : queueName)
                .onItem().ifNull().switchTo(
                        () -> client
                                .queueDeclare(queueName, ic.getQueueDurable(), ic.getQueueExclusive(), ic.getQueueAutoDelete(),
                                        queueArgs)
                                .onItem().invoke(() -> log.queueEstablished(queueName))
                                .onFailure().invoke(ex -> log.unableToEstablishQueue(queueName, ex))
                                .onItem().transformToMulti(v -> establishBindings(client, ic))
                                // What follows is just to "rejoin" the Multi stream into a single Uni emitting the queue name
                                .onCompletion().invoke(() -> Multi.createFrom().item("ignore"))
                                .onItem().ignoreAsUni().onItem().transform(v -> queueName));
    }

    /**
     * Returns a stream that will create bindings from the queue to the exchange with each of the
     * supplied routing keys.
     *
     * @param client the {@link RabbitMQClient} to use
     * @param ic the incoming channel configuration
     * @return a stream of routing keys
     */
    private Multi<String> establishBindings(
            final RabbitMQClient client,
            final RabbitMQConnectorIncomingConfiguration ic) {
        final String exchangeName = getExchangeName(ic);
        final String queueName = ic.getQueueName();
        final List<String> routingKeys = Arrays.stream(ic.getRoutingKeys().split(","))
                .map(String::trim).collect(Collectors.toList());

        return Multi.createFrom().iterable(routingKeys)
                .onItem().call(routingKey -> client.queueBind(queueName, exchangeName, routingKey))
                .onItem().invoke(routingKey -> log.bindingEstablished(queueName, exchangeName, routingKey))
                .onFailure().invoke(ex -> log.unableToEstablishBinding(queueName, exchangeName, ex));
    }

    private RabbitMQFailureHandler createFailureHandler(RabbitMQConnectorIncomingConfiguration config) {
        String strategy = config.getFailureStrategy();
        RabbitMQFailureHandler.Strategy actualStrategy = RabbitMQFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new RabbitMQFailStop(this, config.getChannel());
            case ACCEPT:
                return new RabbitMQAccept(config.getChannel());
            case REJECT:
                return new RabbitMQReject(config.getChannel());
            default:
                throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }

    }

    private RabbitMQAckHandler createAckHandler(RabbitMQConnectorIncomingConfiguration ic) {
        return (Boolean.TRUE.equals(ic.getAutoAcknowledgement())) ? new RabbitMQAutoAck(ic.getChannel())
                : new RabbitMQAck(ic.getChannel());
    }

    public void reportIncomingFailure(String channel, Throwable reason) {
        log.failureReported(channel, reason);
        incomingChannelStatus.put(channel, ChannelStatus.NOT_CONNECTED);
        terminate(null);
    }
}
