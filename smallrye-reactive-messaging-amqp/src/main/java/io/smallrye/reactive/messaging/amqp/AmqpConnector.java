package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.amqp.i18n.AMQPExceptions.ex;
import static io.smallrye.reactive.messaging.amqp.i18n.AMQPLogging.log;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;
import static java.time.Duration.ofSeconds;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.reactive.messaging.amqp.fault.*;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.amqp.AmqpSender;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(AmqpConnector.CONNECTOR_NAME)

@ConnectorAttribute(name = "username", direction = INCOMING_AND_OUTGOING, description = "The username used to authenticate to the broker", type = "string", alias = "amqp-username")
@ConnectorAttribute(name = "password", direction = INCOMING_AND_OUTGOING, description = "The password used to authenticate to the broker", type = "string", alias = "amqp-password")
@ConnectorAttribute(name = "host", direction = INCOMING_AND_OUTGOING, description = "The broker hostname", type = "string", alias = "amqp-host", defaultValue = "localhost")
@ConnectorAttribute(name = "port", direction = INCOMING_AND_OUTGOING, description = "The broker port", type = "int", alias = "amqp-port", defaultValue = "5672")
@ConnectorAttribute(name = "use-ssl", direction = INCOMING_AND_OUTGOING, description = "Whether the AMQP connection uses SSL/TLS", type = "boolean", alias = "amqp-use-ssl", defaultValue = "false")
@ConnectorAttribute(name = "reconnect-attempts", direction = INCOMING_AND_OUTGOING, description = "The number of reconnection attempts", type = "int", alias = "amqp-reconnect-attempts", defaultValue = "100")
@ConnectorAttribute(name = "reconnect-interval", direction = INCOMING_AND_OUTGOING, description = "The interval in second between two reconnection attempts", type = "int", alias = "amqp-reconnect-interval", defaultValue = "10")
@ConnectorAttribute(name = "connect-timeout", direction = INCOMING_AND_OUTGOING, description = "The connection timeout in milliseconds", type = "int", alias = "amqp-connect-timeout", defaultValue = "1000")
@ConnectorAttribute(name = "container-id", direction = INCOMING_AND_OUTGOING, description = "The AMQP container id", type = "string")
@ConnectorAttribute(name = "address", direction = INCOMING_AND_OUTGOING, description = "The AMQP address. If not set, the channel name is used", type = "string")
@ConnectorAttribute(name = "link-name", direction = INCOMING_AND_OUTGOING, description = "The name of the link. If not set, the channel name is used.", type = "string")
@ConnectorAttribute(name = "client-options-name", direction = INCOMING_AND_OUTGOING, description = "The name of the AMQP Client Option bean used to customize the AMQP client configuration", type = "string", alias = "amqp-client-options-name")

@ConnectorAttribute(name = "broadcast", direction = INCOMING, description = "Whether the received AMQP messages must be dispatched to multiple _subscribers_", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "durable", direction = INCOMING, description = "Whether AMQP subscription is durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "auto-acknowledgement", direction = INCOMING, description = "Whether the received AMQP messages must be acknowledged when received", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = INCOMING, description = "Specify the failure strategy to apply when a message produced from an AMQP message is nacked. Accepted values are `fail` (default), `accept`, `release`, `reject`, `modified-failed`, `modified-failed-undeliverable-here`", defaultValue = "fail")

@ConnectorAttribute(name = "durable", direction = OUTGOING, description = "Whether sent AMQP messages are marked durable", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "ttl", direction = OUTGOING, description = "The time-to-live of the send AMQP messages. 0 to disable the TTL", type = "long", defaultValue = "0")
@ConnectorAttribute(name = "credit-retrieval-period", direction = OUTGOING, description = "The period (in milliseconds) between two attempts to retrieve the credits granted by the broker. This time is used when the sender run out of credits.", type = "int", defaultValue = "2000")
@ConnectorAttribute(name = "use-anonymous-sender", direction = OUTGOING, description = "Whether or not the connector should use an anonymous sender.", type = "boolean", defaultValue = "true")

public class AmqpConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, HealthReporter {

    static final String CONNECTOR_NAME = "smallrye-amqp";

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    private Instance<AmqpClientOptions> clientOptions;

    private final List<AmqpClient> clients = new CopyOnWriteArrayList<>();

    private final Map<String, Boolean> opened = new ConcurrentHashMap<>();

    void setup(ExecutionHolder executionHolder) {
        this.executionHolder = executionHolder;
    }

    AmqpConnector() {
        // used for proxies
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Multi<? extends Message<?>> getStreamOfMessages(AmqpReceiver receiver,
            ConnectionHolder holder,
            String address,
            AmqpFailureHandler onNack) {
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
                    Multi<? extends Message<?>> stream = receiver.toMulti()
                            .map(m -> new AmqpMessage<>(m, holder.getContext(), onNack));
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

        AmqpFailureHandler onNack = createFailureHandler(ic);

        Multi<? extends Message<?>> multi = holder.getOrEstablishConnection()
                .onItem().transformToUni(connection -> connection.createReceiver(address, new AmqpReceiverOptions()
                        .setAutoAcknowledgement(autoAck)
                        .setDurable(durable)
                        .setLinkName(link)))
                .onItem().invoke(r -> opened.put(ic.getChannel(), true))
                .onItem().transformToMulti(r -> getStreamOfMessages(r, holder, address, onNack));

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
        boolean useAnonymousSender = oc.getUseAnonymousSender();

        opened.put(oc.getChannel(), false);

        AtomicReference<AmqpSender> sender = new AtomicReference<>();
        AmqpClient client = AmqpClientHelper.createClient(this, oc, clientOptions);
        String link = oc.getLinkName().orElseGet(oc::getChannel);
        ConnectionHolder holder = new ConnectionHolder(client, oc, getVertx());

        Uni<AmqpSender> getSender = Uni.createFrom().item(sender.get())
                .onItem().ifNull().switchTo(() -> {

                    // If we already have a sender, use it.
                    AmqpSender current = sender.get();
                    if (current != null && !current.connection().isDisconnected()) {
                        return Uni.createFrom().item(current);
                    }

                    return holder.getOrEstablishConnection()
                            .onItem().transformToUni(connection -> {
                                if (useAnonymousSender) {
                                    return connection.createAnonymousSender();
                                } else {
                                    return connection.createSender(configuredAddress,
                                            new AmqpSenderOptions().setLinkName(link));
                                }
                            })
                            .onItem().invoke(s -> {
                                sender.set(s);
                                opened.put(oc.getChannel(), true);
                            });
                })
                // If the downstream cancels or on failure, drop the sender.
                .onFailure().invoke(t -> {
                    sender.set(null);
                    opened.put(oc.getChannel(), false);
                })
                .on().cancellation(() -> {
                    sender.set(null);
                    opened.put(oc.getChannel(), false);
                });

        AmqpCreditBasedSender processor = new AmqpCreditBasedSender(
                this,
                holder,
                oc,
                getSender);

        return ReactiveStreams.<Message<?>> builder()
                .via(processor)
                .onError(t -> opened.put(oc.getChannel(), false))
                .ignore();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        clients.forEach(c -> c.close().subscribeAsCompletionStage());
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

    @Override
    public HealthReport getReadiness() {
        return getHealth();
    }

    private HealthReport getHealth() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (Map.Entry<String, Boolean> entry : opened.entrySet()) {
            builder.add(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        return getHealth();
    }

    public void reportFailure(String channel, Throwable reason) {
        log.failureReported(channel, reason);
        opened.put(channel, false);
        terminate(null);
    }
}
