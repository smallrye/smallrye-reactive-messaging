package ${package};

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import ${package}.api.BrokerClient;
import io.smallrye.mutiny.Uni;

public class ${connectorPrefix}FailureHandler {

    private final BrokerClient client;

    static ${connectorPrefix}FailureHandler create(BrokerClient client) {
        return new ${connectorPrefix}FailureHandler(client);
    }

    public ${connectorPrefix}FailureHandler(BrokerClient client) {
        this.client = client;
    }

    public CompletionStage<Void> handle(${connectorPrefix}Message<?> msg, Throwable reason, Metadata metadata) {
        return Uni.createFrom().completionStage(() -> client.reject(msg.getConsumedMessage(), reason.getMessage()))
                .emitOn(msg::runOnMessageContext)
                .subscribeAsCompletionStage();
    }
}
