package ${package};

import java.util.concurrent.CompletionStage;

import ${package}.api.BrokerClient;
import io.smallrye.mutiny.Uni;

public class ${connectorPrefix}AckHandler {

    private final BrokerClient client;

    static ${connectorPrefix}AckHandler create(BrokerClient client) {
        return new ${connectorPrefix}AckHandler(client);
    }

    public ${connectorPrefix}AckHandler(BrokerClient client) {
        this.client = client;
    }

    public CompletionStage<Void> handle(${connectorPrefix}Message<?> msg) {
        return Uni.createFrom().completionStage(client.ack(msg.getConsumedMessage()))
                .emitOn(msg::runOnMessageContext)
                .subscribeAsCompletionStage();
    }
}
