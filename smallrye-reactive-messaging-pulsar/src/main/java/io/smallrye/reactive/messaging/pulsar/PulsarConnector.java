package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarExceptions.ex;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.event.Reception;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.eclipse.microprofile.reactive.messaging.spi.IncomingConnectorFactory;
import org.eclipse.microprofile.reactive.messaging.spi.OutgoingConnectorFactory;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.SubscriberBuilder;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "service-url", type = "string", defaultValue = "pulsar://localhost:6650", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The service URL for the Pulsar service")
@ConnectorAttribute(name = "topic", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The consumed / populated Pulsar topic. If not set, the channel name is used")
@ConnectorAttribute(name = "schema", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The Pulsar schema type of this channel. When configured a schema is built with the given SchemaType and used for the channel. When absent, the schema is resolved searching for a CDI bean typed `Schema` qualified with `@Identifier` and the channel name. As a fallback AUTO_CONSUME or AUTO_PRODUCE are used.")
@ConnectorAttribute(name = "subscription.name", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "The name of the subscription. If not set, a unique, generated id is used")
@ConnectorAttribute(name = "subscription.type", type = "org.apache.pulsar.client.api.SubscriptionType", direction = ConnectorAttribute.Direction.INCOMING, description = "The type of the subscription. If not set, 'Exclusive' will be used as the default")
@ConnectorAttribute(name = "ack.timeout", type = "long", direction = ConnectorAttribute.Direction.INCOMING, description = "Set the timeout for unacknowledged messages in ms. The timeout needs to be greater than 1000 milliseconds (1 second)")
@ConnectorAttribute(name = "ack.group-time", type = "long", direction = ConnectorAttribute.Direction.INCOMING, description = "The interval (in milliseconds) at which the Pulsar consumer sends the ACKs to the server. By default, the consumer will use a 100ms grouping time to send out the acknowledgments to the broker. Setting a group time of 0, will send out the acknowledgments immediately. A longer ack group time will be more efficient at the expense of a slight increase in message re-deliveries after a failure", defaultValue = "100")
@ConnectorAttribute(name = "dead-letter-policy.max-redeliver-count", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "Maximum number of times that a message will be redelivered before being sent to the dead letter topic")
@ConnectorAttribute(name = "dead-letter-policy.dead-letter-topic", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the dead letter topic where the failing messages will be sent")
@ConnectorAttribute(name = "dead-letter-policy.retry-letter-topic", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the retry topic where the failing messages will be sent")
@ConnectorAttribute(name = "max-pending-messages", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "The maximum size of a queue holding pending messages, i.e messages waiting to receive an acknowledgment from a broker", defaultValue = "1000")
public class PulsarConnector implements IncomingConnectorFactory, OutgoingConnectorFactory, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-pulsar";

    private final Map<String, PulsarClient> clients = new ConcurrentHashMap<>();
    private final List<PulsarOutgoingChannel<?>> outgoingChannels = new CopyOnWriteArrayList<>();
    private final List<PulsarIncomingChannel<?>> incomingChannels = new CopyOnWriteArrayList<>();

    @Inject
    private ExecutionHolder executionHolder;

    @Inject
    @Any
    private Instance<Schema<?>> schemas;

    private Vertx vertx;

    private SchemaResolver schemaResolver;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
        this.schemaResolver = new SchemaResolver(schemas);
    }

    @Override
    public PublisherBuilder<? extends Message<?>> getPublisherBuilder(Config config) {
        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config);

        PulsarClient client = clients.computeIfAbsent(clientHash(ic), new PulsarClientCreator(ic));

        try {
            PulsarIncomingChannel<?> channel = new PulsarIncomingChannel<>(client, vertx, schemaResolver.getSchema(ic), ic);
            incomingChannels.add(channel);
            return channel.getPublisher();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildConsumer(e);
        }
    }

    @Override
    public SubscriberBuilder<? extends Message<?>, Void> getSubscriberBuilder(Config config) {
        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config);

        PulsarClient client = clients.computeIfAbsent(clientHash(oc), new PulsarClientCreator(oc));

        try {
            PulsarOutgoingChannel<?> channel = new PulsarOutgoingChannel<>(client, schemaResolver.getSchema(oc), oc);
            outgoingChannels.add(channel);
            return channel.getSubscriber();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildProducer(e);
        }
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        incomingChannels.forEach(PulsarIncomingChannel::close);
        outgoingChannels.forEach(PulsarOutgoingChannel::close);
        for (PulsarClient client : clients.values()) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.unableToCloseClient(e);
            }
        }
        incomingChannels.clear();
        outgoingChannels.clear();
        clients.clear();
    }

    // the idea is to share clients if possible since one PulsarClient can be used for multiple producers and consumers
    private String clientHash(PulsarConnectorCommonConfiguration pulsarConnectorCommonConfiguration) {
        return HashUtil.sha256(pulsarConnectorCommonConfiguration.getServiceUrl());
    }

    private static class PulsarClientCreator implements Function<String, PulsarClient> {
        private final PulsarConnectorCommonConfiguration configuration;

        public PulsarClientCreator(PulsarConnectorCommonConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public PulsarClient apply(String s) {
            try {
                // TODO we need a hell of a lot more configuration here
                return PulsarClient.builder().serviceUrl(configuration.getServiceUrl()).build();
            } catch (PulsarClientException e) {
                throw ex.illegalStateUnableToBuildClient(e);
            }
        }
    }

}
