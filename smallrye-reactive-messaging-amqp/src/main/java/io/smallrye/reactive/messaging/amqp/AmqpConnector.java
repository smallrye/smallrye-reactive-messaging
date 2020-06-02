package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;
import static java.time.Duration.ofSeconds;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.BroadcastProcessor;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connectors.ExecutionHolder;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.amqp.impl.AmqpMessageBuilderImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;
import io.vertx.mutiny.amqp.AmqpReceiver;
import io.vertx.mutiny.amqp.AmqpSender;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;

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

@ConnectorAttribute(name = "durable", direction = OUTGOING, description = "Whether sent AMQP messages are marked durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "ttl", direction = OUTGOING, description = "The time-to-live of the send AMQP messages. 0 to disable the TTL", type = "long", defaultValue = "0")
@ConnectorAttribute(name = "use-anonymous-sender", direction = OUTGOING, description = "Whether or not the connector should use an anonymous sender.", type = "boolean", defaultValue = "true")

public class AmqpConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnector.class);

    static final String CONNECTOR_NAME = "smallrye-amqp";

    private static final String JSON_CONTENT_TYPE = "application/json";

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    private Instance<AmqpClientOptions> clientOptions;

    private final List<AmqpClient> clients = new CopyOnWriteArrayList<>();
    // Needed for testing

    private final Map<String, Boolean> ready = new ConcurrentHashMap<>();

    void setup(ExecutionHolder executionHolder) {
        this.executionHolder = executionHolder;
    }

    AmqpConnector() {
        // used for proxies
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Multi<? extends Message<?>> getStreamOfMessages(AmqpReceiver receiver, ConnectionHolder holder, String address) {
        LOGGER.info("AMQP Receiver listening address {}", address);
        // The processor is used to inject AMQP Connection failure in the stream and trigger a retry.
        BroadcastProcessor processor = BroadcastProcessor.create();
        receiver.exceptionHandler(t -> {
            LOGGER.error("AMQP Receiver error", t);
            processor.onError(t);
        });
        holder.onFailure(processor::onError);

        return Multi.createFrom().deferred(
                () -> {
                    Multi<? extends Message<?>> stream = receiver.toMulti().map(AmqpMessage::new);
                    return Multi.createBy().merging().streams(stream, processor);
                });
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        AmqpConnectorIncomingConfiguration ic = new AmqpConnectorIncomingConfiguration(config);
        String address = ic.getAddress().orElseGet(ic::getChannel);
        boolean broadcast = ic.getBroadcast();
        boolean durable = ic.getDurable();
        boolean autoAck = ic.getAutoAcknowledgement();

        AmqpClient client = AmqpClientHelper.createClient(this, ic, clientOptions);
        String link = ic.getLinkName().orElseGet(ic::getChannel);
        ConnectionHolder holder = new ConnectionHolder(client, ic);

        Multi<? extends Message<?>> multi = holder.getOrEstablishConnection()
                .onItem().produceUni(connection -> connection.createReceiver(address, new AmqpReceiverOptions()
                        .setAutoAcknowledgement(autoAck)
                        .setDurable(durable)
                        .setLinkName(link)))
                .onItem().invoke(r -> ready.put(ic.getChannel(), true))
                .onItem().produceMulti(r -> getStreamOfMessages(r, holder, address));

        Integer interval = ic.getReconnectInterval();
        Integer attempts = ic.getReconnectAttempts();
        multi = multi
                // Retry on failure.
                .onFailure().invoke(t -> LOGGER.error("Unable to retrieve messages from AMQP, retrying...", t))
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(interval)).atMost(attempts)
                .onFailure().invoke(t -> LOGGER.error("Unable to retrieve messages from AMQP, no more retry", t));

        if (broadcast) {
            multi = multi.broadcast().toAllSubscribers();
        }

        return ReactiveStreams.fromPublisher(multi);
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        AmqpConnectorOutgoingConfiguration oc = new AmqpConnectorOutgoingConfiguration(config);
        String configuredAddress = oc.getAddress().orElseGet(oc::getChannel);
        boolean durable = oc.getDurable();
        long ttl = oc.getTtl();
        boolean useAnonymousSender = oc.getUseAnonymousSender();

        AtomicReference<AmqpSender> sender = new AtomicReference<>();
        AmqpClient client = AmqpClientHelper.createClient(this, oc, clientOptions);
        String link = oc.getLinkName().orElseGet(oc::getChannel);
        ConnectionHolder holder = new ConnectionHolder(client, oc);

        Uni<AmqpSender> getSender = Uni.createFrom().item(sender.get())
                .onItem().ifNull().switchTo(() -> {

                    // If we already have a sender, use it.

                    AmqpSender current = sender.get();
                    if (current != null && !current.connection().isDisconnected()) {
                        return Uni.createFrom().item(current);
                    }

                    return holder.getOrEstablishConnection()
                            .onItem().produceUni(connection -> {
                                if (useAnonymousSender) {
                                    return connection.createAnonymousSender();
                                } else {
                                    return connection.createSender(configuredAddress,
                                            new AmqpSenderOptions().setLinkName(link));
                                }
                            })
                            .onItem().invoke(s -> {
                                sender.set(s);
                                ready.put(oc.getChannel(), true);
                            });
                })
                // If the downstream cancels or on failure, drop the sender.
                .onFailure().invoke(t -> sender.set(null))
                .on().cancellation(() -> sender.set(null));

        return ReactiveStreams.<Message<?>> builder().flatMapCompletionStage(message -> getSender
                .flatMap(s -> send(s, message, durable, ttl, configuredAddress, useAnonymousSender, oc))

                // Depending on the failure, we complete smoothly or propagate the failure which would trigger a retry.
                .onFailure().recoverWithUni(failure -> {
                    if (clients.isEmpty()) {
                        LOGGER.error("The AMQP message has not been sent, the client is closed");
                        return Uni.createFrom().item(message);
                    } else {
                        LOGGER.error("Unable to send the AMQP message", failure);
                        return Uni.createFrom().failure(failure);
                    }
                })
                .onFailure().retry()
                .withBackOff(ofSeconds(1), ofSeconds(oc.getReconnectInterval())).atMost(oc.getReconnectAttempts())

                .subscribeAsCompletionStage()).ignore();
    }

    private String getActualAddress(Message<?> message, io.vertx.mutiny.amqp.AmqpMessage amqp, String configuredAddress,
            boolean isAnonymousSender) {
        String address = amqp.address();
        if (address != null) {
            if (isAnonymousSender) {
                return address;
            } else {
                LOGGER.warn(
                        "Unable to use the address configured in the message ({}) - the connector is not using an anonymous sender, using {} instead",
                        address, configuredAddress);
                return configuredAddress;
            }

        }

        return message.getMetadata(OutgoingAmqpMetadata.class)
                .flatMap(o -> {
                    String addressFromMessage = o.getAddress();
                    if (addressFromMessage != null && !isAnonymousSender) {
                        LOGGER.warn(
                                "Unable to use the address configured in the message ({}) - the connector is not using an anonymous sender, using {} instead",
                                addressFromMessage, configuredAddress);
                        return Optional.empty();
                    }
                    return Optional.ofNullable(addressFromMessage);
                })
                .orElse(configuredAddress);
    }

    private Uni<Message<?>> send(AmqpSender sender, Message<?> msg, boolean durable, long ttl, String configuredAddress,
            boolean isAnonymousSender, AmqpConnectorCommonConfiguration configuration) {
        int retryAttempts = configuration.getReconnectAttempts();
        int retryInterval = configuration.getReconnectInterval();
        io.vertx.mutiny.amqp.AmqpMessage amqp;
        if (msg instanceof AmqpMessage) {
            amqp = ((AmqpMessage<?>) msg).getAmqpMessage();
        } else if (msg.getPayload() instanceof io.vertx.mutiny.amqp.AmqpMessage) {
            amqp = (io.vertx.mutiny.amqp.AmqpMessage) msg.getPayload();
        } else if (msg.getPayload() instanceof io.vertx.amqp.AmqpMessage) {
            amqp = new io.vertx.mutiny.amqp.AmqpMessage((io.vertx.amqp.AmqpMessage) msg.getPayload());
        } else {
            amqp = convertToAmqpMessage(msg, durable, ttl);
        }

        String actualAddress = getActualAddress(msg, amqp, configuredAddress, isAnonymousSender);
        if (clients.isEmpty()) {
            LOGGER.error("The AMQP message to address `{}` has not been sent, the client is closed",
                    actualAddress);
            return Uni.createFrom().item(msg);
        }

        if (!actualAddress.equals(amqp.address())) {
            amqp = new io.vertx.mutiny.amqp.AmqpMessage(
                    new AmqpMessageBuilderImpl(amqp.getDelegate()).address(actualAddress).build());
        }

        LOGGER.debug("Sending AMQP message to address `{}` ",
                actualAddress);
        return sender.sendWithAck(amqp)
                .onFailure().retry().withBackOff(ofSeconds(1), ofSeconds(retryInterval)).atMost(retryAttempts)
                .onItem().produceCompletionStage(x -> msg.ack())
                .onItem().apply(x -> msg);
    }

    private io.vertx.mutiny.amqp.AmqpMessage convertToAmqpMessage(Message<?> message, boolean durable, long ttl) {
        Object payload = message.getPayload();
        Optional<OutgoingAmqpMetadata> metadata = message.getMetadata(OutgoingAmqpMetadata.class);
        AmqpMessageBuilder builder = io.vertx.mutiny.amqp.AmqpMessage.create();

        if (durable) {
            builder.durable(true);
        } else {
            builder.durable(metadata.map(OutgoingAmqpMetadata::isDurable).orElse(false));
        }

        if (ttl > 0) {
            builder.ttl(ttl);
        } else {
            long t = metadata.map(OutgoingAmqpMetadata::getTtl).orElse(-1L);
            if (t > 0) {
                builder.ttl(t);
            }
        }

        if (payload instanceof String) {
            builder.withBody((String) payload);
        } else if (payload instanceof Boolean) {
            builder.withBooleanAsBody((Boolean) payload);
        } else if (payload instanceof Buffer) {
            builder.withBufferAsBody((Buffer) payload);
        } else if (payload instanceof Byte) {
            builder.withByteAsBody((Byte) payload);
        } else if (payload instanceof Character) {
            builder.withCharAsBody((Character) payload);
        } else if (payload instanceof Double) {
            builder.withDoubleAsBody((Double) payload);
        } else if (payload instanceof Float) {
            builder.withFloatAsBody((Float) payload);
        } else if (payload instanceof Instant) {
            builder.withInstantAsBody((Instant) payload);
        } else if (payload instanceof Integer) {
            builder.withIntegerAsBody((Integer) payload);
        } else if (payload instanceof JsonArray) {
            builder.withJsonArrayAsBody((JsonArray) payload)
                    .contentType(JSON_CONTENT_TYPE);
        } else if (payload instanceof JsonObject) {
            builder.withJsonObjectAsBody((JsonObject) payload)
                    .contentType(JSON_CONTENT_TYPE);
        } else if (payload instanceof Long) {
            builder.withLongAsBody((Long) payload);
        } else if (payload instanceof Short) {
            builder.withShortAsBody((Short) payload);
        } else if (payload instanceof UUID) {
            builder.withUuidAsBody((UUID) payload);
        } else if (payload instanceof byte[]) {
            builder.withBufferAsBody(Buffer.buffer((byte[]) payload));
        } else {
            builder.withBufferAsBody(new Buffer(Json.encodeToBuffer(payload)))
                    .contentType(JSON_CONTENT_TYPE);
        }

        metadata.ifPresent(new Consumer<OutgoingAmqpMetadata>() {
            @Override
            public void accept(OutgoingAmqpMetadata meta) {
                if (meta.getAddress() != null) {
                    builder.address(meta.getAddress());
                }
                if (meta.getProperties() != null && !meta.getProperties().isEmpty()) {
                    builder.applicationProperties(meta.getProperties());
                }
                if (meta.getContentEncoding() != null) {
                    builder.contentEncoding(meta.getContentEncoding());
                }
                if (meta.getContentType() != null) {
                    builder.contentType(meta.getContentType());
                }
                if (meta.getCorrelationId() != null) {
                    builder.correlationId(meta.getCorrelationId());
                }
                if (meta.getId() != null) {
                    builder.id(meta.getId());
                }
                if (meta.getGroupId() != null) {
                    builder.groupId(meta.getGroupId());
                }
                if (meta.getPriority() >= 0) {
                    builder.priority((short) meta.getPriority());
                }
                if (meta.getSubject() != null) {
                    builder.subject(meta.getSubject());
                }
            }
        });
        return builder.build();
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

    public boolean isReady(String channel) {
        return ready.getOrDefault(channel, false);
    }
}
