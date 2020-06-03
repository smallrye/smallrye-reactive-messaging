package io.smallrye.reactive.messaging.mqtt;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

public class MqttFailStop implements MqttFailureHandler {

    private final Logger logger;
    private final String channel;

    public MqttFailStop(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(Throwable reason) {
        logger.error("A message sent to channel `{}` has been nacked, fail-stop", channel);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        return future;
    }
}
