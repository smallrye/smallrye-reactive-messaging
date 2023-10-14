package connectors;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

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
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.vertx.mutiny.core.Vertx;

@ApplicationScoped
@Connector(MyConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "client-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "The client id ", mandatory = true)
@ConnectorAttribute(name = "buffer-size", type = "int", direction = INCOMING, description = "The size buffer of incoming messages waiting to be processed", defaultValue = "128")
@ConnectorAttribute(name = "topic", type = "string", direction = OUTGOING, description = "The default topic to send the messages, defaults to channel name if not set")
@ConnectorAttribute(name = "maxPendingMessages", type = "int", direction = OUTGOING, description = "The maximum size of a queue holding pending messages", defaultValue = "1000")
@ConnectorAttribute(name = "waitForWriteCompletion", type = "boolean", direction = OUTGOING, description = "Whether the outgoing channel waits for the write completion", defaultValue = "true")
public class MyConnector implements InboundConnector, OutboundConnector {

    public static final String CONNECTOR_NAME = "smallrye-my-connector";

    @Inject
    ExecutionHolder executionHolder;

    Vertx vertx;

    List<MyIncomingChannel> incomingChannels = new CopyOnWriteArrayList<>();
    List<MyOutgoingChannel> outgoingChannels = new CopyOnWriteArrayList<>();

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
