package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MqttFailStop implements MqttFailureHandler {

    private final String channel;

    public MqttFailStop(String channel) {
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(Throwable reason) {
        log.messageNackedFailStop(channel);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        return future;
    }
}
