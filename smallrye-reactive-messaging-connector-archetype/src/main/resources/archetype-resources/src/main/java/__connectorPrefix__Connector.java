package ${package};

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ${package}.api.BrokerClient;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;

import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(${connectorPrefix}Connector.CONNECTOR_NAME)
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = ConnectorAttribute.Direction.INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "client-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "The client id ", mandatory = true)
@ConnectorAttribute(name = "buffer-size", type = "int", direction = INCOMING, description = "The size buffer of incoming messages waiting to be processed", defaultValue = "128")
@ConnectorAttribute(name = "topic", type = "string", direction = OUTGOING, description = "The default topic to send the messages, defaults to channel name if not set")
@ConnectorAttribute(name = "maxPendingMessages", type = "int", direction = OUTGOING, description = "The maximum size of a queue holding pending messages", defaultValue = "1000")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = OUTGOING, description = "Whether the outgoing channel waits for the write completion", defaultValue = "true")
public class ${connectorPrefix}Connector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-${connectorName}";

    @Inject
    ExecutionHolder executionHolder;

    Vertx vertx;

    List<${connectorPrefix}IncomingChannel> incomingChannels = new CopyOnWriteArrayList<>();
    List<${connectorPrefix}OutgoingChannel> outgoingChannels = new CopyOnWriteArrayList<>();

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        ${connectorPrefix}ConnectorIncomingConfiguration ic = new ${connectorPrefix}ConnectorIncomingConfiguration(config);
        String channelName = ic.getChannel();
        String clientId = ic.getClientId();
        int bufferSize = ic.getBufferSize();
        // ...
        BrokerClient client = BrokerClient.create(clientId);
        ${connectorPrefix}IncomingChannel channel = new ${connectorPrefix}IncomingChannel(vertx, ic, client);
        incomingChannels.add(channel);
        return channel.getStream();
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        ${connectorPrefix}ConnectorOutgoingConfiguration oc = new ${connectorPrefix}ConnectorOutgoingConfiguration(config);
        String channelName = oc.getChannel();
        String clientId = oc.getClientId();
        int pendingMessages = oc.getMaxPendingMessages();
        // ...
        BrokerClient client = BrokerClient.create(clientId);
        ${connectorPrefix}OutgoingChannel channel = new ${connectorPrefix}OutgoingChannel(vertx, oc, client);
        outgoingChannels.add(channel);
        return channel.getSubscriber();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (${connectorPrefix}IncomingChannel channel : incomingChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (${connectorPrefix}OutgoingChannel channel : outgoingChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (${connectorPrefix}IncomingChannel channel : incomingChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (${connectorPrefix}OutgoingChannel channel : outgoingChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (${connectorPrefix}IncomingChannel channel : incomingChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (${connectorPrefix}OutgoingChannel channel : outgoingChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }
}
