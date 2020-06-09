package io.smallrye.reactive.messaging.mqtt;

import static io.smallrye.reactive.messaging.mqtt.i18n.MqttLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MqttIgnoreFailure implements MqttFailureHandler {

    private final String channel;

    public MqttIgnoreFailure(String channel) {
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(Throwable reason) {
        // We commit the message, log and continue

        log.messageNackedIgnore(channel, reason.getMessage());
        log.messageNackedFullIgnored(reason);
        return CompletableFuture.completedFuture(null);
    }
}
