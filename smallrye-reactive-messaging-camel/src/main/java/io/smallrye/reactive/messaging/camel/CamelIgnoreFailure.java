package io.smallrye.reactive.messaging.camel;

import static io.smallrye.reactive.messaging.camel.i18n.CamelLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CamelIgnoreFailure implements CamelFailureHandler {

    private final String channel;

    public CamelIgnoreFailure(String channel) {
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(CamelMessage<?> message, Throwable reason) {
        // We commit the message, log and continue

        log.messageNackedIgnore(channel, reason.getMessage());
        log.messageNackedFullIgnored(reason);
        message.getExchange().setException(reason);
        message.getExchange().setRollbackOnly(true);
        return CompletableFuture.completedFuture(null);
    }
}
