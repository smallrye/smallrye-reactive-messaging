package io.smallrye.reactive.messaging.kafka.commit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.mutiny.core.Vertx;

/**
 * Will commit the record offset received by the Kafka consumer (if higher than the previously committed offset).
 * This offset may be greater than the currently ACKed message.
 * <p>
 * This handler is the default when `enable.auto.commit` is `false`.
 * This strategy provides at-least-once delivery if the channel processes the message without performing
 * any asynchronous processing.
 * <p>
 * This strategy should not be used on high-load as offset commit is expensive.
 * <p>
 * To use set `commit-strategy` to `latest`.
 */
public class KafkaLatestCommit extends ContextHolder implements KafkaCommitHandler {

    private final KafkaConsumer<?, ?> consumer;

    /**
     * Stores the last offset for each topic/partition.
     * This map must always be accessed from the same thread (Vert.x context).
     */
    private final Map<TopicPartition, Long> offsets = new HashMap<>();

    public KafkaLatestCommit(Vertx vertx, io.vertx.mutiny.kafka.client.consumer.KafkaConsumer<?, ?> consumer) {
        super(vertx);
        this.consumer = (KafkaConsumer<?, ?>) consumer.getDelegate();
    }

    @Override
    public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        runOnContext(() -> {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            TopicPartition key = new TopicPartition(record.getTopic(), record.getPartition());
            Long last = offsets.get(key);
            // Verify that the latest committed offset before this one.
            if (last == null || last < record.getOffset() + 1) {
                offsets.put(key, record.getOffset() + 1);
                map.put(key, new OffsetAndMetadata(record.getOffset() + 1, null));

                consumer.commit(map, ar -> {
                    if (ar.failed()) {
                        future.completeExceptionally(ar.cause());
                    } else {
                        future.complete(null);
                    }
                });
            } else {
                future.complete(null);
            }
        });

        return future;
    }
}
