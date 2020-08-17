package io.smallrye.reactive.messaging.kafka.commit;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;

/**
 * Ignores an ACK and does not commit any offsets.
 *
 * This handler is the default when `enable.auto.commit` is `true`.
 *
 * When `enable.auto.commit` is `true` this strategy provides at-least-once delivery
 * if the channel processes the message without performing any asynchronous processing.
 *
 * To use set `commit-strategy` to `ignore`.
 */
public class KafkaIgnoreCommit implements KafkaCommitHandler {

    @Override
    public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
        return CompletableFuture.completedFuture(null);
    }
}
