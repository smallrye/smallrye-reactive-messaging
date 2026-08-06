package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarExceptions.ex;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarLogging.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.opentelemetry.api.OpenTelemetry;
import io.smallrye.mutiny.Uni;
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

@ConnectorAttribute(name = "graceful-shutdown", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING, description = "Whether a graceful shutdown should be attempted when the application terminates.", defaultValue = "false")

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
@ConnectorAttribute(name = "max-inflight-messages", type = "int", direction = ConnectorAttribute.Direction.OUTGOING, description = "The maximum size of a queue holding pending messages, i.e messages waiting to receive an acknowledgment from a broker. Defaults to 1000 messages")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = ConnectorAttribute.Direction.OUTGOING, description = "Whether the client waits for the broker to acknowledge the written record before acknowledging the message", defaultValue = "true")
public class PulsarConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-pulsar";

    private final Map<String, PulsarClientEntry> clients = new ConcurrentHashMap<>();
    private final Map<String, String> channelHashes = new ConcurrentHashMap<>();
    private final Map<String, PulsarOutgoingChannel<?>> outgoingChannels = new ConcurrentHashMap<>();
    private final Map<String, PulsarIncomingChannel<?>> incomingChannels = new ConcurrentHashMap<>();

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

    @Inject
    private Instance<OpenTelemetry> openTelemetryInstance;

    /**
     * Optional CDI bean providing shared resources (event loop group, DNS resolver) across all Pulsar clients.
     * If not provided, the connector creates and manages a default instance.
     */
    @Inject
    private Instance<PulsarClientSharedResources> sharedResourcesInstance;

    private PulsarClientSharedResources sharedResources;
    private boolean managedSharedResources;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
        if (sharedResourcesInstance.isResolvable()) {
            this.sharedResources = sharedResourcesInstance.get();
            this.managedSharedResources = false;
        } else {
            this.sharedResources = PulsarClientSharedResources.builder()
                    .resourceTypes(PulsarClientSharedResources.SharedResource.EventLoopGroup,
                            PulsarClientSharedResources.SharedResource.DnsResolver)
                    .build();
            this.managedSharedResources = true;
        }
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        PulsarConnectorIncomingConfiguration ic = new PulsarConnectorIncomingConfiguration(config);

        ClientConfigurationData clientConf = configResolver.getClientConf(ic);
        String hash = clientHash(clientConf);
        PulsarClientEntry entry = clients.computeIfAbsent(hash,
                x -> new PulsarClientEntry(createPulsarClient(ic, clientConf)))
                .retain(ic.getChannel());
        channelHashes.put(ic.getChannel(), hash);

        try {
            PulsarIncomingChannel<?> channel = new PulsarIncomingChannel<>(entry.client, vertx, schemaResolver.getSchema(ic),
                    CDIUtils.getInstanceById(ackHandlerFactories, ic.getAckStrategy()).get(),
                    CDIUtils.getInstanceById(failureHandlerFactories, ic.getFailureStrategy()).get(),
                    ic, configResolver, openTelemetryInstance);
            incomingChannels.put(ic.getChannel(), channel);
            return channel.getPublisher();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildConsumer(e);
        }
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        PulsarConnectorOutgoingConfiguration oc = new PulsarConnectorOutgoingConfiguration(config);

        ClientConfigurationData clientConf = configResolver.getClientConf(oc);
        String hash = clientHash(clientConf);
        PulsarClientEntry entry = clients.computeIfAbsent(hash,
                x -> new PulsarClientEntry(createPulsarClient(oc, clientConf)))
                .retain(oc.getChannel());
        channelHashes.put(oc.getChannel(), hash);

        try {
            PulsarOutgoingChannel<?> channel = new PulsarOutgoingChannel<>(entry.client, schemaResolver.getSchema(oc), oc,
                    configResolver, openTelemetryInstance);
            outgoingChannels.put(oc.getChannel(), channel);
            return channel.getSubscriber();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildProducer(e);
        }
    }

    // the idea is to share clients if possible since one PulsarClient can be used for multiple producers and consumers
    private String clientHash(ClientConfigurationData clientConf) {
        return HashUtil.sha256(clientConf.toString());
    }

    @Override
    public CompletionStage<Void> shutdownIncoming(String channel) {
        PulsarIncomingChannel<?> incoming = incomingChannels.remove(channel);
        if (incoming != null) {
            return Uni.createFrom().completionStage(incoming::closeAsync)
                    .call(() -> Uni.createFrom().completionStage(() -> releaseClient(channel)))
                    .subscribeAsCompletionStage();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> shutdownOutgoing(String channel) {
        PulsarOutgoingChannel<?> outgoing = outgoingChannels.remove(channel);
        if (outgoing != null) {
            return Uni.createFrom().completionStage(outgoing::closeAsync)
                    .call(() -> Uni.createFrom().completionStage(() -> releaseClient(channel)))
                    .subscribeAsCompletionStage();
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> releaseClient(String channel) {
        String hash = channelHashes.remove(channel);
        if (hash == null) {
            return CompletableFuture.completedFuture(null);
        }
        AtomicReference<PulsarClient> toClose = new AtomicReference<>();
        clients.compute(hash, (key, entry) -> {
            if (entry == null) {
                return null;
            }
            if (entry.release(channel)) {
                toClose.compareAndSet(null, entry.client);
                return null;
            }
            return entry;
        });
        PulsarClient toCloseClient = toClose.get();
        if (toCloseClient != null) {
            return toClose.get().closeAsync();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> terminate() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (PulsarIncomingChannel<?> c : incomingChannels.values()) {
            futures.add(c.closeAsync());
        }
        for (PulsarOutgoingChannel<?> c : outgoingChannels.values()) {
            futures.add(c.closeAsync());
        }
        for (PulsarClientEntry entry : clients.values()) {
            futures.add(entry.client.closeAsync());
        }
        incomingChannels.clear();
        outgoingChannels.clear();
        clients.clear();
        channelHashes.clear();
        if (managedSharedResources && sharedResources != null) {
            try {
                sharedResources.close();
            } catch (PulsarClientException e) {
                log.unableToCloseClient(e);
            }
        }
        if (futures.isEmpty()) {
            return CompletableFuture.completedStage(null);
        }
        return Uni.combine().all().unis(futures.stream().map(Uni.createFrom()::completionStage).toList())
                .discardItems()
                .subscribeAsCompletionStage();
    }

    private PulsarClient createPulsarClient(PulsarConnectorCommonConfiguration cc, ClientConfigurationData configuration) {
        try {
            var builder = configResolver.configure(cc, configuration);
            log.createdClientWithConfig(builder.getClientConfigurationData());
            return builder
                    .sharedResources(sharedResources)
                    .build();
        } catch (PulsarClientException e) {
            throw ex.illegalStateUnableToBuildClient(e);
        }
    }

    public PulsarClient getClient(String channel) {
        String hash = channelHashes.get(channel);
        if (hash == null) {
            return null;
        }
        PulsarClientEntry entry = clients.get(hash);
        return entry != null ? entry.client : null;
    }

    @SuppressWarnings("unchecked")
    public <T> Consumer<T> getConsumer(String channel) {
        PulsarIncomingChannel<?> incoming = incomingChannels.get(channel);
        if (incoming != null) {
            return ((Consumer<T>) incoming.getConsumer());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public <T> Producer<T> getProducer(String channel) {
        PulsarOutgoingChannel<?> outgoing = outgoingChannels.get(channel);
        if (outgoing != null) {
            return ((Producer<T>) outgoing.getProducer());
        }
        return null;
    }

    public Set<String> getConsumerChannels() {
        return incomingChannels.keySet();
    }

    public Set<String> getProducerChannels() {
        return outgoingChannels.keySet();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PulsarIncomingChannel<?> incomingChannel : incomingChannels.values()) {
            incomingChannel.isStarted(builder);
        }
        for (PulsarOutgoingChannel<?> outgoingChannel : outgoingChannels.values()) {
            outgoingChannel.isStarted(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PulsarIncomingChannel<?> incomingChannel : incomingChannels.values()) {
            incomingChannel.isReady(builder);
        }
        for (PulsarOutgoingChannel<?> outgoingChannel : outgoingChannels.values()) {
            outgoingChannel.isReady(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PulsarIncomingChannel<?> incomingChannel : incomingChannels.values()) {
            incomingChannel.isAlive(builder);
        }
        for (PulsarOutgoingChannel<?> outgoingChannel : outgoingChannels.values()) {
            outgoingChannel.isAlive(builder);
        }
        return builder.build();
    }

    static class PulsarClientEntry {
        final PulsarClient client;
        private final Set<String> channels = ConcurrentHashMap.newKeySet();

        PulsarClientEntry(PulsarClient client) {
            this.client = client;
        }

        PulsarClientEntry retain(String channel) {
            channels.add(channel);
            return this;
        }

        boolean release(String channel) {
            channels.remove(channel);
            return channels.isEmpty();
        }

    }
}
