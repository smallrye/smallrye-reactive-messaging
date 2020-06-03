package io.smallrye.reactive.messaging.amqp.fault;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

import io.smallrye.reactive.messaging.amqp.AmqpMessage;
import io.vertx.mutiny.core.Context;

public class AmqpReject implements AmqpFailureHandler {

    private final Logger logger;
    private final String channel;

    public AmqpReject(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public <V> CompletionStage<Void> handle(AmqpMessage<V> msg, Context context, Throwable reason) {
        // We mark the message as rejected and fail.
        logger.warn("A message sent to channel `{}` has been nacked, ignoring the failure and mark the message as rejected",
                channel);
        logger.debug("The full ignored failure is", reason);
        CompletableFuture<Void> future = new CompletableFuture<>();
        context.runOnContext(x -> {
            msg.getAmqpMessage().rejected();
            future.complete(null);
        });
        return future;
    }
}
