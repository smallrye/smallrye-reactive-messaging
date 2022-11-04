package io.smallrye.reactive.messaging.kafka.commit;

import static io.smallrye.reactive.messaging.kafka.i18n.KafkaLogging.log;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.impl.TopicPartitions;
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

    private KafkaConsumer<?, ?> consumer;

    /**
     * Stores the last offset for each topic/partition.
     * This map must always be accessed from the same thread (Vert.x context).
     */
    private final Map<TopicPartition, Long> offsets = new HashMap<>();

    @ApplicationScoped
    @Identifier(Strategy.LATEST)
    public static class Factory implements KafkaCommitHandler.Factory {

        @Override
        public KafkaLatestCommit create(
                KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new KafkaLatestCommit(vertx, config, consumer);
        }
    }

    public KafkaLatestCommit(Vertx vertx, KafkaConnectorIncomingConfiguration configuration,
            KafkaConsumer<?, ?> consumer) {
        super(vertx, configuration.config()
                .getOptionalValue(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.class).orElse(60000));
        this.consumer = consumer;
    }

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        runOnContext(() -> {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            TopicPartition key = TopicPartitions.getTopicPartition(record);
            Long last = offsets.get(key);
            // Verify that the latest committed offset before this one.
            if (last == null || last < record.getOffset() + 1) {
                offsets.put(key, record.getOffset() + 1);
                map.put(key, new OffsetAndMetadata(record.getOffset() + 1, null));
                consumer.commitAsync(map)
                        .subscribe().with(ignored -> {
                        }, throwable -> log.failedToCommitAsync(key, record.getOffset() + 1));
            }
        });
        return Uni.createFrom().voidItem().runSubscriptionOn(record::runOnMessageContext);
    }
}
