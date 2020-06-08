package io.smallrye.reactive.messaging.kafka.fault;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

public class KafkaFailStop implements KafkaFailureHandler {

    private final Logger logger;
    private final String channel;

    public KafkaFailStop(Logger logger, String channel) {
        this.logger = logger;
        this.channel = channel;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(
            IncomingKafkaRecord<K, V> record, Throwable reason) {
        // We don't commit, we just fail and stop the client.
        logger.error("A message sent to channel `{}` has been nacked, fail-stop", channel);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(reason);
        return future;
    }
}
