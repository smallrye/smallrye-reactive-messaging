package ${package}.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface BrokerClient {

    static BrokerClient create(String clientId) {
        return null;
    }

    CompletionStage<ConsumedMessage<?>> poll();

    CompletionStage<PublishAck> send(SendMessage sendMessage);

    CompletableFuture<Void> ack(ConsumedMessage<?> msg);

    CompletableFuture<Void> reject(ConsumedMessage<?> msg, String reason);

    void close();
}
