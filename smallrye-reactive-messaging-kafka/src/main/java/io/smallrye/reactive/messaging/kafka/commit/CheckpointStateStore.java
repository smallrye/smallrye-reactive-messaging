package io.smallrye.reactive.messaging.kafka.commit;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.vertx.mutiny.core.Vertx;

/**
 * Remote state store for checkpointing Kafka processing state
 */
public interface CheckpointStateStore {

    /**
     * Fetch the latest processing state for given topic-partitions.
     * <p>
     * Called on Vert.x event loop context
     *
     * @param partitions set of topic-partitions
     * @return the {@link Uni} completing with the map of processing state by topic-partition
     */
    Uni<Map<TopicPartition, ProcessingState<?>>> fetchProcessingState(Collection<TopicPartition> partitions);

    /**
     * Persist the given processing state in the state store
     * <p>
     * Called on Vert.x event loop context
     *
     * @param state map of processing state by topic-partition
     * @return the {@link Uni} completing when the persist operation is completed.
     */
    Uni<Void> persistProcessingState(Map<TopicPartition, ProcessingState<?>> state);

    /**
     * Close the state store on channel termination
     */
    default void close() {
        // no implementation
    }

    /**
     * Factory interface for {@link CheckpointStateStore}
     */
    interface Factory {

        /**
         * Create {@link CheckpointStateStore} instance for the given channel
         *
         * @param config the channel config
         * @param vertx the vert.x instance
         * @param consumer the Kafka consumer
         * @param stateType the type of the persisted state, can be null
         * @return the created state store instance
         */
        CheckpointStateStore create(KafkaConnectorIncomingConfiguration config, Vertx vertx,
                KafkaConsumer<?, ?> consumer, Class<?> stateType);
    }
}
