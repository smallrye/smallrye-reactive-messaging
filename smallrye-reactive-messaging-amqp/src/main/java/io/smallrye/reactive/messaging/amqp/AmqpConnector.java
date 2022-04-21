package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;
import static java.time.Duration.ofSeconds;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
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
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.amqp.fault.AmqpAccept;
import io.smallrye.reactive.messaging.amqp.fault.AmqpFailStop;
import io.smallrye.reactive.messaging.amqp.fault.AmqpFailureHandler;
import io.smallrye.reactive.messaging.amqp.fault.AmqpModifiedFailed;
import io.smallrye.reactive.messaging.amqp.fault.AmqpModifiedFailedAndUndeliverableHere;
import io.smallrye.reactive.messaging.amqp.fault.AmqpReject;
import io.smallrye.reactive.messaging.amqp.fault.AmqpRelease;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.amqp.AmqpSender;
import io.vertx.mutiny.core.Vertx;
import io.vertx.proton.ProtonSender;

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
@ConnectorAttribute(name = "tracing-enabled", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "health-timeout", direction = INCOMING_AND_OUTGOING, description = "The max number of seconds to wait to determine if the connection with the broker is still established for the readiness check. After that threshold, the check is considered as failed.", type = "int", defaultValue = "3")
@ConnectorAttribute(name = "cloud-events", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Enables (default) or disables the Cloud Event support. If enabled on an _incoming_ channel, the connector analyzes the incoming records and try to create Cloud Event metadata. If enabled on an _outgoing_, the connector sends the outgoing messages as Cloud Event if the message includes Cloud Event Metadata.", defaultValue = "true")
@ConnectorAttribute(name = "capabilities", type = "string", direction = INCOMING_AND_OUTGOING, description = " A comma-separated list of capabilities proposed by the sender or receiver client.")

@ConnectorAttribute(name = "broadcast", direction = INCOMING, description = "Whether the received AMQP messages must be dispatched to multiple _subscribers_", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "durable", direction = INCOMING, description = "Whether AMQP subscription is durable", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "auto-acknowledgement", direction = INCOMING, description = "Whether the received AMQP messages must be acknowledged when received", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = INCOMING, description = "Specify the failure strategy to apply when a message produced from an AMQP message is nacked. Accepted values are `fail` (default), `accept`, `release`, `reject`, `modified-failed`, `modified-failed-undeliverable-here`", defaultValue = "fail")

@ConnectorAttribute(name = "durable", direction = OUTGOING, description = "Whether sent AMQP messages are marked durable", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "ttl", direction = OUTGOING, description = "The time-to-live of the send AMQP messages. 0 to disable the TTL", type = "long", defaultValue = "0")
@ConnectorAttribute(name = "credit-retrieval-period", direction = OUTGOING, description = "The period (in milliseconds) between two attempts to retrieve the credits granted by the broker. This time is used when the sender run out of credits.", type = "int", defaultValue = "2000")
@ConnectorAttribute(name = "use-anonymous-sender", direction = OUTGOING, description = "Whether or not the connector should use an anonymous sender. Default value is `true` if the broker supports it, `false` otherwise. If not supported, it is not possible to dynamically change the destination address.", type = "boolean")
@ConnectorAttribute(name = "merge", direction = OUTGOING, description = "Whether the connector should allow multiple upstreams", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "cloud-events-source", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Configure the default `source` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `source` attribute itself", alias = "cloud-events-default-source")
@ConnectorAttribute(name = "cloud-events-type", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Configure the default `type` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `type` attribute itself", alias = "cloud-events-default-type")
@ConnectorAttribute(name = "cloud-events-subject", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Configure the default `subject` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `subject` attribute itself", alias = "cloud-events-default-subject")
@ConnectorAttribute(name = "cloud-events-data-content-type", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Configure the default `datacontenttype` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `datacontenttype` attribute itself", alias = "cloud-events-default-data-content-type")
@ConnectorAttribute(name = "cloud-events-data-schema", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Configure the default `dataschema` attribute of the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `dataschema` attribute itself", alias = "cloud-events-default-data-schema")
@ConnectorAttribute(name = "cloud-events-insert-timestamp", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "Whether or not the connector should insert automatically the `time` attribute into the outgoing Cloud Event. Requires `cloud-events` to be set to `true`. This value is used if the message does not configure the `time` attribute itself", alias = "cloud-events-default-timestamp", defaultValue = "true")
@ConnectorAttribute(name = "cloud-events-mode", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "The Cloud Event mode (`structured` or `binary` (default)). Indicates how are written the cloud events in the outgoing record", defaultValue = "binary")

public class AmqpConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-amqp";

    static Tracer TRACER;

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    @Any
    private Instance<AmqpClientOptions> clientOptions;

    private final List<AmqpClient> clients = new CopyOnWriteArrayList<>();

    /**
     * Tracks the processor used to send messages to AMQP.
     * The map is used for cleanup and health checks.
     */
    private final Map<String, AmqpCreditBasedSender> processors = new ConcurrentHashMap<>();

    /**
     * Tracks the state of the AMQP connection.
     * The boolean is set to {@code true} when the connection with the AMQP broker is established, and set to
     * {@code false} once the connection is lost and retry attempts have failed.
     * This map is used for health check:
     *
     * <ul>
     * <li>liveness: failed if opened is set to false for a channel</li>
     * </li>readiness: failed if opened is set to false for a channel</li>
     * </ul>
     */
    private final Map<String, Boolean> opened = new ConcurrentHashMap<>();

    /**
     * Tracks the consumer connection holder.
     * This map is used for cleanup and health checks.
     */
    private final Map<String, ConnectionHolder> holders = new ConcurrentHashMap<>();

    void setup(ExecutionHolder executionHolder) {
        this.executionHolder = executionHolder;
    }

    AmqpConnector() {
        // used for proxies
    }

    @PostConstruct
    void init() {
        TRACER = GlobalOpenTelemetry.getTracerProvider().get("io.smallrye.reactive.messaging.amqp");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Multi<? extends Message<?>> getStreamOfMessages(AmqpReceiver receiver,
            ConnectionHolder holder,
            String address,
            String channel,
            AmqpFailureHandler onNack,
            boolean cloudEventEnabled,
            Boolean tracingEnabled) {
        log.receiverListeningAddress(address);

        // The processor is used to inject AMQP Connection failure in the stream and trigger a retry.
        BroadcastProcessor processor = BroadcastProcessor.create();
        receiver.exceptionHandler(t -> {
            log.receiverError(t);
            processor.onError(t);
        });
        holder.onFailure(processor::onError);

        return Multi.createFrom().deferred(
                () -> {
                    Multi<AmqpMessage<?>> stream = receiver.toMulti()
                            .onItem().transformToUniAndConcatenate(m -> {
                                try {
                                    return Uni.createFrom().item(new AmqpMessage<>(m, holder.getContext(), onNack,
                                            cloudEventEnabled, tracingEnabled));
                                } catch (Exception e) {
                                    log.unableToCreateMessage(channel, e);
                                    return Uni.createFrom().nullItem();
                                }
                            });

                    if (tracingEnabled) {
                        stream = stream.onItem().invoke(this::incomingTrace);
                    }

                    return Multi.createBy().merging().streams(stream, processor);
                });
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        AmqpConnectorIncomingConfiguration ic = new AmqpConnectorIncomingConfiguration(config);
        String address = ic.getAddress().orElseGet(ic::getChannel);

        opened.put(ic.getChannel(), false);

        boolean broadcast = ic.getBroadcast();
        boolean durable = ic.getDurable();
        boolean autoAck = ic.getAutoAcknowledgement();

        AmqpClient client = AmqpClientHelper.createClient(this, ic, clientOptions);
        String link = ic.getLinkName().orElseGet(ic::getChannel);
        ConnectionHolder holder = new ConnectionHolder(client, ic, getVertx());
        holders.put(ic.getChannel(), holder);

        AmqpFailureHandler onNack = createFailureHandler(ic);

        Multi<? extends Message<?>> multi = holder.getOrEstablishConnection()
                .onItem().transformToUni(connection -> connection.createReceiver(address, new AmqpReceiverOptions()
                        .setAutoAcknowledgement(autoAck)
                        .setDurable(durable)
                        .setLinkName(link)
                        .setCapabilities(getClientCapabilities(ic))))
                .onItem().invoke(r -> opened.put(ic.getChannel(), true))
                .onItem().transformToMulti(r -> getStreamOfMessages(r, holder, address, ic.getChannel(), onNack,
                        ic.getCloudEvents(), ic.getTracingEnabled()));

        Integer interval = ic.getReconnectInterval();
        Integer attempts = ic.getReconnectAttempts();
        multi = multi
                // Retry on failure.
                .onFailure().invoke(log::retrieveMessagesRetrying)
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(interval)).atMost(attempts)
                .onFailure().invoke(t -> {
                    opened.put(ic.getChannel(), false);
                    log.retrieveMessagesNoMoreRetrying(t);
                });

        if (broadcast) {
            multi = multi.broadcast().toAllSubscribers();
        }

        return ReactiveStreams.fromPublisher(multi);
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        AmqpConnectorOutgoingConfiguration oc = new AmqpConnectorOutgoingConfiguration(config);
        String configuredAddress = oc.getAddress().orElseGet(oc::getChannel);

        opened.put(oc.getChannel(), false);

        AtomicReference<AmqpSender> sender = new AtomicReference<>();
        AmqpClient client = AmqpClientHelper.createClient(this, oc, clientOptions);
        String link = oc.getLinkName().orElseGet(oc::getChannel);
        ConnectionHolder holder = new ConnectionHolder(client, oc, getVertx());

        Uni<AmqpSender> getSender = Uni.createFrom().deferred(() -> {

            // If we already have a sender, use it.
            AmqpSender current = sender.get();
            if (current != null && !current.connection().isDisconnected()) {
                if (isLinkOpen(current)) {
                    return Uni.createFrom().item(current);
                } else {
                    // link closed, close the sender, and recreate one.
                    current.closeAndForget();
                }
            }

            return holder.getOrEstablishConnection()
                    .onItem().transformToUni(connection -> {
                        boolean anonymous = oc.getUseAnonymousSender()
                                .orElseGet(() -> ConnectionHolder.supportAnonymousRelay(connection));

                        if (anonymous) {
                            return connection.createAnonymousSender();
                        } else {
                            return connection.createSender(configuredAddress,
                                    new AmqpSenderOptions()
                                            .setLinkName(link)
                                            .setCapabilities(getClientCapabilities(oc)));
                        }
                    })
                    .onItem().invoke(s -> {
                        AmqpSender orig = sender.getAndSet(s);
                        if (orig != null) { // Close the previous one if any.
                            orig.closeAndForget();
                        }
                        opened.put(oc.getChannel(), true);
                    });
        })
                // If the downstream cancels or on failure, drop the sender.
                .onFailure().invoke(t -> {
                    sender.set(null);
                    opened.put(oc.getChannel(), false);
                })
                .onCancellation().invoke(() -> {
                    sender.set(null);
                    opened.put(oc.getChannel(), false);
                });

        AmqpCreditBasedSender processor = new AmqpCreditBasedSender(
                this,
                holder,
                oc,
                getSender);
        processors.put(oc.getChannel(), processor);

        return ReactiveStreams.<Message<?>> builder()
                .via(processor)
                .onError(t -> {
                    log.failureReported(oc.getChannel(), t);
                    opened.put(oc.getChannel(), false);
                })
                .ignore();
    }

    private boolean isLinkOpen(AmqpSender current) {
        ProtonSender sender = current.getDelegate().unwrap();
        if (sender == null) {
            return false;
        }
        return sender.isOpen();
    }

    public List<String> getClientCapabilities(AmqpConnectorCommonConfiguration configuration) {
        if (configuration.getCapabilities().isPresent()) {
            String capabilities = configuration.getCapabilities().get();
            return Arrays.stream(capabilities.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        processors.values().forEach(AmqpCreditBasedSender::cancel);
        clients.forEach(AmqpClient::closeAndForget);
        clients.clear();
    }

    public Vertx getVertx() {
        return executionHolder.vertx();
    }

    public void addClient(AmqpClient client) {
        clients.add(client);
    }

    private AmqpFailureHandler createFailureHandler(AmqpConnectorIncomingConfiguration config) {
        String strategy = config.getFailureStrategy();
        AmqpFailureHandler.Strategy actualStrategy = AmqpFailureHandler.Strategy.from(strategy);
        switch (actualStrategy) {
            case FAIL:
                return new AmqpFailStop(this, config.getChannel());
            case ACCEPT:
                return new AmqpAccept(config.getChannel());
            case REJECT:
                return new AmqpReject(config.getChannel());
            case RELEASE:
                return new AmqpRelease(config.getChannel());
            case MODIFIED_FAILED:
                return new AmqpModifiedFailed(config.getChannel());
            case MODIFIED_FAILED_UNDELIVERABLE_HERE:
                return new AmqpModifiedFailedAndUndeliverableHere(config.getChannel());
            default:
                throw ex.illegalArgumentInvalidFailureStrategy(strategy);
        }

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
        for (Map.Entry<String, ConnectionHolder> holder : holders.entrySet()) {
            try {
                builder.add(holder.getKey(), holder.getValue().isConnected().await()
                        .atMost(Duration.ofSeconds(holder.getValue().getHealthTimeout())));
            } catch (Exception e) {
                builder.add(holder.getKey(), false, e.getMessage());
            }
        }

        for (Map.Entry<String, AmqpCreditBasedSender> sender : processors.entrySet()) {
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
        for (Map.Entry<String, Boolean> entry : opened.entrySet()) {
            builder.add(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    @Override
    public HealthReport getStartup() {
        return getLiveness();
    }

    public void reportFailure(String channel, Throwable reason) {
        log.failureReported(channel, reason);
        opened.put(channel, false);
        terminate(null);
    }

    private void incomingTrace(AmqpMessage<?> message) {
        TracingMetadata tracingMetadata = TracingMetadata.fromMessage(message).orElse(TracingMetadata.empty());

        final SpanBuilder spanBuilder = TRACER.spanBuilder(message.getAddress() + " receive")
                .setSpanKind(SpanKind.CONSUMER);

        // Handle possible parent span
        final Context parentSpanContext = tracingMetadata.getPreviousContext();
        if (parentSpanContext != null) {
            spanBuilder.setParent(parentSpanContext);
        } else {
            spanBuilder.setNoParent();
        }

        final Span span = spanBuilder.startSpan();

        // Set Span attributes
        span.setAttribute(SemanticAttributes.MESSAGING_SYSTEM, "AMQP 1.0");
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION, message.getAddress());
        span.setAttribute(SemanticAttributes.MESSAGING_DESTINATION_KIND, "queue");

        // Make available as parent for subsequent spans inside message processing
        span.makeCurrent();

        message.injectTracingMetadata(tracingMetadata.withSpan(span));

        span.end();
    }
}
