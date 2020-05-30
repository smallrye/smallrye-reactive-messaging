package io.smallrye.reactive.messaging.amqp.fault;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;

public class AmqpFailStop implements AmqpFailureHandler {

    private final Logger logger;
    private final String channel;

    public AmqpFailStop(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(AmqpMessage<V> msg, Throwable reason) {
        // We mark the message as rejected and fail.
        logger.error("A message sent to channel `{}` has been nacked, rejecting the message and fail-stop", channel);
        msg.getAmqpMessage().rejected();
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        return future;
    }
}
