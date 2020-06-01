package io.smallrye.reactive.messaging.camel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

public class CamelIgnoreFailure implements CamelFailureHandler {

    private final Logger logger;
    private final String channel;

    public CamelIgnoreFailure(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public CompletionStage<Void> handle(CamelMessage<?> message, Throwable reason) {
        // We commit the message, log and continue
        logger.warn("A message sent to channel `{}` has been nacked, ignored failure is: {}.", channel,
                reason.getMessage());
        logger.debug("The full ignored failure is", reason);
        message.getExchange().setException(reason);
        message.getExchange().setRollbackOnly(true);
        return CompletableFuture.completedFuture(null);
    }
}
