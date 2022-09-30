package io.smallrye.reactive.messaging.kafka.commit;

import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.mutiny.core.Vertx;

/**
 * Ignores an ACK and does not commit any offsets.
 *
 * This handler is the default when `enable.auto.commit` is `true`.
 *
 * When `enable.auto.commit` is `true` this strategy DOES NOT guarantee at-least-once delivery.
 *
 * To use set `commit-strategy` to `ignore`.
 */
public class KafkaIgnoreCommit implements KafkaCommitHandler {

    @ApplicationScoped
    @Identifier(Strategy.IGNORE)
    public static class Factory implements KafkaCommitHandler.Factory {

        @Override
        public KafkaIgnoreCommit create(
                KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new KafkaIgnoreCommit();
        }
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        return Uni.createFrom().voidItem().runSubscriptionOn(record::runOnMessageContext);
    }
}
