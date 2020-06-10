package io.smallrye.reactive.messaging.camel;

import static io.smallrye.reactive.messaging.camel.i18n.CamelLogging.log;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class CamelFailStop implements CamelFailureHandler {

    private final String channel;

    public CamelFailStop(String channel) {
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(CamelMessage<?> message, Throwable reason) {
        log.messageNackedFailStop(channel);
        message.getExchange().setException(reason);
        message.getExchange().setRollbackOnly(true);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        return future;
    }
}
