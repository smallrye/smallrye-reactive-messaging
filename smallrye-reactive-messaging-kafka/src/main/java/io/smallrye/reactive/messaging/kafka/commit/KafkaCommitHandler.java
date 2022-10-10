package io.smallrye.reactive.messaging.kafka.commit;

import java.util.Collection;
import java.util.function.BiConsumer;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.mutiny.core.Vertx;

/**
 * Kafka commit handling strategy
 */
@Experimental("Experimental API")
public interface KafkaCommitHandler {

    /**
     * Identifiers of default strategies
     */
    interface Strategy {
        String LATEST = "latest";
        String IGNORE = "ignore";
        String THROTTLED = "throttled";
        String CHECKPOINT = "checkpoint";

    }

    /**
     * Factory interface for {@link KafkaCommitHandler}
     */
    interface Factory {

        KafkaCommitHandler create(KafkaConnectorIncomingConfiguration config,
                Vertx vertx, KafkaConsumer<?, ?> consumer, BiConsumer<Throwable, Boolean> reportFailure);

    }

    /**
     * Called on message received but before calling the processing function.
     * Returned {@link Uni} allows chaining asynchronous actions before message processing.
     *
     * @param record incoming Kafka record
     * @param <K> type of record key
     * @param <V> type of record value
     * @return the {@link Uni} yielding the received record
     */
    default <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        return Uni.createFrom().item(record);
    }

    /**
     * Called on channel shutdown
     *
     * @param graceful {@code true} if it is a graceful shutdown
     */
    default void terminate(boolean graceful) {
        // Do nothing by default.
    }

    /**
     * Called on partitions assigned on Kafka rebalance listener
     *
     * @param partitions assigned partitions
     */
    default void partitionsAssigned(Collection<TopicPartition> partitions) {
        // Do nothing by default.
    }

    /**
     * Called on partitions revokd on Kafka rebalance listener
     *
     * @param partitions revoked partitions
     */
    default void partitionsRevoked(Collection<TopicPartition> partitions) {
        // Do nothing by default.
    }

    /**
     * Handle message acknowledgment
     *
     * @param record incoming Kafka record
     * @param <K> type of record key
     * @param <V> type of record value
     * @return a completion stage completed when the message is acknowledged.
     */
    <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record);

}
