package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarExceptions.ex;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(PulsarConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "client-configuration", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Identifier of a CDI bean that provides the default Pulsar client configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "serviceUrl", type = "string", defaultValue = "pulsar://localhost:6650", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The service URL for the Pulsar service")
@ConnectorAttribute(name = "topic", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The consumed / populated Pulsar topic. If not set, the channel name is used")
@ConnectorAttribute(name = "schema", type = "string", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "The Pulsar schema type of this channel. When configured a schema is built with the given SchemaType and used for the channel. When absent, the schema is resolved searching for a CDI bean typed `Schema` qualified with `@Identifier` and the channel name. As a fallback AUTO_CONSUME or AUTO_PRODUCE are used.")
@ConnectorAttribute(name = "health-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether health reporting is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")

@ConnectorAttribute(name = "consumer-configuration", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Identifier of a CDI bean that provides the default Pulsar consumer configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "ack-strategy", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Specify the commit strategy to apply when a message produced from a record is acknowledged. Values can be `ack`, `cumulative`.", defaultValue = "ack")
@ConnectorAttribute(name = "failure-strategy", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Specify the failure strategy to apply when a message produced from a record is acknowledged negatively (nack). Values can be `nack` (default), `fail`, `ignore` or `reconsume-later", defaultValue = "nack")
@ConnectorAttribute(name = "reconsumeLater.delay", type = "long", direction = ConnectorAttribute.Direction.INCOMING, description = "Default delay for reconsume failure-strategy, in seconds", defaultValue = "3")
@ConnectorAttribute(name = "negativeAck.redeliveryBackoff", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Comma separated values for configuring negative ack MultiplierRedeliveryBackoff, min delay, max delay, multiplier.")
@ConnectorAttribute(name = "ackTimeout.redeliveryBackoff", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Comma separated values for configuring ack timeout MultiplierRedeliveryBackoff, min delay, max delay, multiplier.")
@ConnectorAttribute(name = "deadLetterPolicy.maxRedeliverCount", type = "int", direction = ConnectorAttribute.Direction.INCOMING, description = "Maximum number of times that a message will be redelivered before being sent to the dead letter topic")
@ConnectorAttribute(name = "deadLetterPolicy.deadLetterTopic", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the dead letter topic where the failing messages will be sent")
@ConnectorAttribute(name = "deadLetterPolicy.retryLetterTopic", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the retry topic where the failing messages will be sent")
@ConnectorAttribute(name = "deadLetterPolicy.initialSubscriptionName", type = "string", direction = ConnectorAttribute.Direction.INCOMING, description = "Name of the initial subscription name of the dead letter topic")
@ConnectorAttribute(name = "batchReceive", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether batch receive is used to consume messages", defaultValue = "false")

@ConnectorAttribute(name = "producer-configuration", type = "string", direction = ConnectorAttribute.Direction.OUTGOING, description = "Identifier of a CDI bean that provides the default Pulsar producer configuration for this channel. The channel configuration can still override any attribute. The bean must have a type of Map<String, Object> and must use the @io.smallrye.common.annotation.Identifier qualifier to set the identifier.")
@ConnectorAttribute(name = "maxPendingMessages", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "The maximum size of a queue holding pending messages, i.e messages waiting to receive an acknowledgment from a broker", defaultValue = "1000")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "Whether the client waits for the broker to acknowledge the written record before acknowledging the message", defaultValue = "true")
public class PulsarConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-pulsar";

    private final Map<ClientConfigurationData, PulsarClient> clients = new ConcurrentHashMap<>();
    private final Map<String, PulsarClient> clientsByChannel = new ConcurrentHashMap<>();
    private final List<PulsarOutgoingChannel<?>> outgoingChannels = new CopyOnWriteArrayList<>();
    private final List<PulsarIncomingChannel<?>> incomingChannels = new CopyOnWriteArrayList<>();

    @Inject
    private ExecutionHolder executionHolder;

    private Vertx vertx;

    @Inject
    private SchemaResolver schemaResolver;

    @Inject
    private ConfigResolver configResolver;

    @Inject
    @Any
    private Instance<PulsarAckHandler.Factory> ackHandlerFactories;

    @Inject
    @Any
    private Instance<PulsarFailureHandler.Factory> failureHandlerFactories;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config);

        PulsarClient client = clients.computeIfAbsent(configResolver.getClientConf(ic), this::createPulsarClient);
        clientsByChannel.put(ic.getChannel(), client);

        try {
            PulsarIncomingChannel<?> channel = new PulsarIncomingChannel<>(client, vertx, schemaResolver.getSchema(ic),
                    CDIUtils.getInstanceById(ackHandlerFactories, ic.getAckStrategy()).get(),
                    CDIUtils.getInstanceById(failureHandlerFactories, ic.getFailureStrategy()).get(),
                    ic, configResolver);
            incomingChannels.add(channel);
            return channel.getPublisher();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildConsumer(e);
        }
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config);

        PulsarClient client = clients.computeIfAbsent(configResolver.getClientConf(oc), this::createPulsarClient);
        clientsByChannel.put(oc.getChannel(), client);

        try {
            PulsarOutgoingChannel<?> channel = new PulsarOutgoingChannel<>(client, schemaResolver.getSchema(oc), oc,
                    configResolver);
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
        clientsByChannel.clear();
    }

    private PulsarClientImpl createPulsarClient(ClientConfigurationData configuration) {
        try {
            log.createdClientWithConfig(configuration);
            return new PulsarClientImpl(configuration, vertx.nettyEventLoopGroup());
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildClient(e);
        }
    }

    public PulsarClient getClient(String channel) {
        return clientsByChannel.get(channel);
    }

    @SuppressWarnings("unchecked")
    public <T> Consumer<T> getConsumer(String channel) {
        return incomingChannels.stream()
                .filter(ks -> ks.getChannel().equals(channel))
                .map(incomingChannel -> ((Consumer<T>) incomingChannel.getConsumer()))
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    public <T> Producer<T> getProducer(String channel) {
        return outgoingChannels.stream()
                .filter(ks -> ks.getChannel().equals(channel))
                .map(outgoingChannel -> ((Producer<T>) outgoingChannel.getProducer()))
                .findFirst().orElse(null);
    }

    public Set<String> getConsumerChannels() {
        return incomingChannels.stream().map(PulsarIncomingChannel::getChannel).collect(Collectors.toSet());
    }

    public Set<String> getProducerChannels() {
        return outgoingChannels.stream().map(PulsarOutgoingChannel::getChannel).collect(Collectors.toSet());
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PulsarIncomingChannel<?> incomingChannel : incomingChannels) {
            incomingChannel.isStarted(builder);
        }
        for (PulsarOutgoingChannel<?> outgoingChannel : outgoingChannels) {
            outgoingChannel.isStarted(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PulsarIncomingChannel<?> incomingChannel : incomingChannels) {
            incomingChannel.isReady(builder);
        }
        for (PulsarOutgoingChannel<?> outgoingChannel : outgoingChannels) {
            outgoingChannel.isReady(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PulsarIncomingChannel<?> incomingChannel : incomingChannels) {
            incomingChannel.isAlive(builder);
        }
        for (PulsarOutgoingChannel<?> outgoingChannel : outgoingChannels) {
            outgoingChannel.isAlive(builder);
        }
        return builder.build();
    }
}
