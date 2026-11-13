package connectors;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import connectors.api.BrokerClient;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;

// <lifecycle>
@ApplicationScoped
public class MyConnectorLifecycle implements InboundConnector, OutboundConnector {

    private final Map<String, MyIncomingChannel> incomingChannels = new ConcurrentHashMap<>();
    private final Map<String, MyOutgoingChannel> outgoingChannels = new ConcurrentHashMap<>();
    private final Map<String, BrokerClient> clients = new ConcurrentHashMap<>();

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        // ... create channel, track it by name ...
        return null;
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        // ... create channel, track it by name ...
        return null;
    }

    @Override
    public CompletionStage<Void> prepareCloseIncoming(String channel) {
        // Called after in-flight messages have been drained.
        // Flush commits, complete handlers while the client is alive.
        MyIncomingChannel incoming = incomingChannels.get(channel);
        if (incoming != null) {
            incoming.close();
        }
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> closeIncoming(String channel) {
        // Close the consumer and release channel resources.
        incomingChannels.remove(channel);
        BrokerClient client = clients.remove(channel);
        if (client != null) {
            client.close();
        }
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> closeOutgoing(String channel) {
        outgoingChannels.remove(channel);
        BrokerClient client = clients.remove(channel);
        if (client != null) {
            client.close();
        }
        return CompletableFuture.completedStage(null);
    }

    @Override
    public CompletionStage<Void> terminate() {
        // Called once after all channels have been closed.
        // Clean up connector-wide resources.
        clients.values().forEach(BrokerClient::close);
        clients.clear();
        return CompletableFuture.completedStage(null);
    }
}
// </lifecycle>
