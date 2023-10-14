package connectors;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import connectors.api.BrokerClient;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

// <health-report>
@ApplicationScoped
@Connector(MyConnectorWithPartials.CONNECTOR_NAME)
public class MyConnectorWithPartials implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-my-connector";

    List<MyIncomingChannel> incomingChannels = new CopyOnWriteArrayList<>();
    List<MyOutgoingChannel> outgoingChannels = new CopyOnWriteArrayList<>();

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (MyIncomingChannel channel : incomingChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (MyOutgoingChannel channel : outgoingChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (MyIncomingChannel channel : incomingChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (MyOutgoingChannel channel : outgoingChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (MyIncomingChannel channel : incomingChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (MyOutgoingChannel channel : outgoingChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    // </health-report>

    @Inject
    ExecutionHolder executionHolder;

    Vertx vertx;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        MyConnectorIncomingConfiguration ic = new MyConnectorIncomingConfiguration(config);
        String channelName = ic.getChannel();
        String clientId = ic.getClientId();
        int bufferSize = ic.getBufferSize();
        // ...
        BrokerClient client = BrokerClient.create(clientId);
        MyIncomingChannel channel = new MyIncomingChannel(vertx, ic, client);
        incomingChannels.add(channel);
        return channel.getStream();
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        MyConnectorOutgoingConfiguration oc = new MyConnectorOutgoingConfiguration(config);
        String channelName = oc.getChannel();
        String clientId = oc.getClientId();
        int pendingMessages = oc.getMaxPendingMessages();
        // ...
        BrokerClient client = BrokerClient.create(clientId);
        MyOutgoingChannel channel = new MyOutgoingChannel(vertx, oc, client);
        outgoingChannels.add(channel);
        return channel.getSubscriber();
    }
}
