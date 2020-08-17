package io.smallrye.reactive.messaging.kafka.commit;

import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 * Will commit to the latest offset received by the Kafka consumer.
 * This offset may be greater than the currently ACKed message.
 *
 * This handler is the default when `enable.auto.commit` is `false`.
 *
 * This strategy does not guarantee at-least-once delivery.
 *
 * To use set `commit-strategy` to `latest`.
 */
public class KafkaLatestCommit implements KafkaCommitHandler {

    private final KafkaConsumer<?, ?> consumer;

    public KafkaLatestCommit(KafkaConsumer<?, ?> consumer) {
        this.consumer = consumer;
    }

    @Override
    public <K, V> CompletionStage<Void> handle(IncomingKafkaRecord<K, V> record) {
        return consumer.commit().subscribeAsCompletionStage();
    }
}
