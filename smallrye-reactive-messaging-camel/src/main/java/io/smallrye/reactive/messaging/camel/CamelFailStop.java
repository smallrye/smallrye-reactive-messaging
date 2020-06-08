package io.smallrye.reactive.messaging.camel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

public class CamelFailStop implements CamelFailureHandler {

    private final Logger logger;
    private final String channel;

    public CamelFailStop(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(CamelMessage<?> message, Throwable reason) {
        logger.error("A message sent to channel `{}` has been nacked, fail-stop", channel);
        message.getExchange().setException(reason);
        message.getExchange().setRollbackOnly(true);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        return future;
    }
}
