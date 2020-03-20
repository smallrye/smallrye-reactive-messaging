package io.smallrye.reactive.messaging.amqp;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.literal.NamedLiteral;
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
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.amqp.impl.AmqpMessageBuilderImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpClient;
import io.vertx.mutiny.amqp.AmqpConnection;
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
@ConnectorAttribute(name = "containerId", direction = INCOMING_AND_OUTGOING, description = "The AMQP container id", type = "string")
@ConnectorAttribute(name = "address", direction = INCOMING_AND_OUTGOING, description = "The AMQP address. If not set, the channel name is used", type = "string")
@ConnectorAttribute(name = "client-options-name", direction = INCOMING_AND_OUTGOING, description = "The name of the AMQP Client Option bean used to customize the AMQP client configuration", type = "string", alias = "amqp-client-options-name")

@ConnectorAttribute(name = "broadcast", direction = INCOMING, description = "Whether the received AMQP messages must be dispatched to multiple _subscribers_", type = "boolean", defaultValue = "false")
@ConnectorAttribute(name = "durable", direction = INCOMING, description = "Whether AMQP subscription is durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "auto-acknowledgement", direction = INCOMING, description = "Whether the received AMQP messages must be acknowledged when received", type = "boolean", defaultValue = "false")

@ConnectorAttribute(name = "durable", direction = OUTGOING, description = "Whether sent AMQP messages are marked durable", type = "boolean", defaultValue = "true")
@ConnectorAttribute(name = "ttl", direction = OUTGOING, description = "The time-to-live of the send AMQP messages. 0 to disable the TTL", type = "long", defaultValue = "0")

public class AmqpConnector implements IncomingConnectorFactory, OutgoingConnectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnector.class);
    static final String CONNECTOR_NAME = "smallrye-amqp";

    @Inject
    private Instance<Vertx> instanceOfVertx;

    @Inject
    private Instance<AmqpClientOptions> clientOptions;

    private boolean internalVertxInstance = false;
    private Vertx vertx;
    private final List<AmqpClient> clients = new CopyOnWriteArrayList<>();

    public void terminate(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        if (internalVertxInstance) {
            vertx.close().await().indefinitely();
        }
    }

    @PostConstruct
    void init() {
        if (instanceOfVertx == null || instanceOfVertx.isUnsatisfied()) {
            internalVertxInstance = true;
            this.vertx = Vertx.vertx();
        } else {
            this.vertx = instanceOfVertx.get();
        }
    }

    AmqpConnector() {
        this.vertx = null;
    }

    private AmqpClient createClient(AmqpConnectorCommonConfiguration config) {
        AmqpClient client;
        Optional<String> clientOptionsName = config.getClientOptionsName();
        if (clientOptionsName.isPresent()) {
            client = createClientFromClientOptionsBean(clientOptionsName.get());
        } else {
            client = getClient(config);
        }
        clients.add(client);
        return client;
    }

    private AmqpClient createClientFromClientOptionsBean(String optionsBeanName) {
        Instance<AmqpClientOptions> options = clientOptions.select(NamedLiteral.of(optionsBeanName));
        if (options.isUnsatisfied()) {
            throw new IllegalStateException(
                    "Cannot find a " + AmqpClientOptions.class.getName() + " bean named " + optionsBeanName);
        }
        LOGGER.debug("Creating AMQP client from bean named '{}'", optionsBeanName);
        return AmqpClient.create(new io.vertx.mutiny.core.Vertx(vertx.getDelegate()), options.get());
    }

    private synchronized AmqpClient getClient(AmqpConnectorCommonConfiguration config) {
        try {
            String username = config.getUsername().orElse(null);
            String password = config.getPassword().orElse(null);
            String host = config.getHost();
            int port = config.getPort();
            LOGGER.info("AMQP broker configured to {}:{} for channel {}", host, port, config.getChannel());
            boolean useSsl = config.getUseSsl();
            int reconnectAttempts = config.getReconnectAttempts();
            int reconnectInterval = config.getReconnectInterval();
            int connectTimeout = config.getConnectTimeout();
            String containerId = config.getContainerId().orElse(null);

            AmqpClientOptions options = new AmqpClientOptions()
                    .setUsername(username)
                    .setPassword(password)
                    .setHost(host)
                    .setPort(port)
                    .setContainerId(containerId)
                    .setSsl(useSsl)
                    .setReconnectAttempts(reconnectAttempts)
                    .setReconnectInterval(reconnectInterval)
                    .setConnectTimeout(connectTimeout);
            return AmqpClient.create(new io.vertx.mutiny.core.Vertx(vertx.getDelegate()), options);
        } catch (Exception e) {
            LOGGER.error("Unable to create client", e);
            throw new IllegalStateException("Unable to create a client, probably a config error", e);
        }
    }

    private Multi<? extends Message<?>> getStreamOfMessages(AmqpReceiver receiver) {
        return Multi.createFrom().deferred(
                () -> receiver.toMulti()
                        .map(m -> new AmqpMessage<>(m)));
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        AmqpConnectorIncomingConfiguration ic = new AmqpConnectorIncomingConfiguration(config);
        String address = ic.getAddress().orElseGet(ic::getChannel);
        boolean broadcast = ic.getBroadcast();
        boolean durable = ic.getDurable();
        boolean autoAck = ic.getAutoAcknowledgement();
        Uni<AmqpReceiver> uni = createClient(ic)
                .connect()
                .flatMap(connection -> connection.createReceiver(address, new AmqpReceiverOptions()
                        .setAutoAcknowledgement(autoAck)
                        .setDurable(durable)));

        PublisherBuilder<? extends Message<?>> builder = ReactiveStreams
                .fromCompletionStage(uni.subscribeAsCompletionStage())
                .flatMapRsPublisher(this::getStreamOfMessages);

        if (broadcast) {
            return ReactiveStreams.fromPublisher(
                    Multi.createFrom().publisher(builder.buildRs())
                            .broadcast().toAllSubscribers());
        }

        return builder;
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        AmqpConnectorOutgoingConfiguration oc = new AmqpConnectorOutgoingConfiguration(config);
        String configuredAddress = oc.getAddress().orElseGet(oc::getChannel);
        boolean durable = oc.getDurable();
        long ttl = oc.getTtl();

        AtomicReference<AmqpSender> sender = new AtomicReference<>();
        AmqpClient client = createClient(oc);
        return ReactiveStreams.<Message<?>> builder().flatMapCompletionStage(message -> {
            AmqpSender as = sender.get();
            if (as == null) {
                return client.connect()
                        .flatMap(AmqpConnection::createAnonymousSender)
                        .map(s -> {
                            sender.set(s);
                            return s;
                        })
                        .flatMap(s -> send(s, message, durable, ttl, configuredAddress))
                        .onFailure().invoke(failure -> {
                            if (clients.isEmpty()) {
                                LOGGER.error("The AMQP message has not been sent, the client is closed");
                            } else {
                                LOGGER.error("Unable to send the AMQP message", failure);
                            }
                        }).subscribeAsCompletionStage();
            } else {
                return send(as, message, durable, ttl, configuredAddress).subscribeAsCompletionStage();
            }
        }).ignore();
    }

    private String getActualAddress(Message<?> message, io.vertx.mutiny.amqp.AmqpMessage amqp, String configuredAddress) {
        if (amqp.address() != null) {
            return amqp.address();
        }
        return message.getMetadata(OutgoingAmqpMetadata.class)
                .flatMap(o -> Optional.ofNullable(o.getAddress()))
                .orElse(configuredAddress);
    }

    private Uni<Message> send(AmqpSender sender, Message msg, boolean durable, long ttl, String configuredAddress) {
        io.vertx.mutiny.amqp.AmqpMessage amqp;
        if (msg instanceof AmqpMessage) {
            amqp = ((AmqpMessage) msg).getAmqpMessage();
        } else if (msg.getPayload() instanceof io.vertx.mutiny.amqp.AmqpMessage) {
            amqp = (io.vertx.mutiny.amqp.AmqpMessage) msg.getPayload();
        } else if (msg.getPayload() instanceof io.vertx.amqp.AmqpMessage) {
            amqp = new io.vertx.mutiny.amqp.AmqpMessage((io.vertx.amqp.AmqpMessage) msg.getPayload());
        } else {
            amqp = convertToAmqpMessage(msg, durable, ttl);
        }

        String actualAddress = getActualAddress(msg, amqp, configuredAddress);
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
                .onItem().<Void> produceCompletionStage(x -> msg.ack())
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
            builder.withJsonArrayAsBody((JsonArray) payload);
        } else if (payload instanceof JsonObject) {
            builder.withJsonObjectAsBody((JsonObject) payload);
        } else if (payload instanceof Long) {
            builder.withLongAsBody((Long) payload);
        } else if (payload instanceof Short) {
            builder.withShortAsBody((Short) payload);
        } else if (payload instanceof UUID) {
            builder.withUuidAsBody((UUID) payload);
        } else {
            builder.withBody(payload.toString());
        }

        builder.address(metadata.map(OutgoingAmqpMetadata::getAddress).orElse(null));
        builder.applicationProperties(metadata.map(OutgoingAmqpMetadata::getProperties).orElseGet(JsonObject::new));

        builder.contentEncoding(metadata.map(OutgoingAmqpMetadata::getContentEncoding).orElse(null));
        builder.contentType(metadata.map(OutgoingAmqpMetadata::getContentType).orElse(null));
        builder.correlationId(metadata.map(OutgoingAmqpMetadata::getCorrelationId).orElse(null));
        builder.groupId(metadata.map(OutgoingAmqpMetadata::getGroupId).orElse(null));
        builder.id(metadata.map(OutgoingAmqpMetadata::getId).orElse(null));
        int priority = metadata.map(OutgoingAmqpMetadata::getPriority).orElse(-1);
        if (priority >= 0) {
            builder.priority((short) priority);
        }
        builder.subject(metadata.map(OutgoingAmqpMetadata::getSubject).orElse(null));
        return builder.build();
    }

    @PreDestroy
    public synchronized void close() {
        clients.forEach(c -> c.close().subscribeAsCompletionStage());
        clients.clear();
    }
}
