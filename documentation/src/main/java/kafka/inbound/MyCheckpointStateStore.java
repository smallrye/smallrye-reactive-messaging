package kafka.inbound;

import java.util.Collection;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.commit.CheckpointStateStore;
import io.smallrye.reactive.messaging.kafka.commit.ProcessingState;
import io.vertx.mutiny.core.Vertx;

public class MyCheckpointStateStore implements CheckpointStateStore {

    private final String consumerGroupId;
    private final Class<?> stateType;

    public MyCheckpointStateStore(String consumerGroupId, Class<?> stateType) {
        this.consumerGroupId = consumerGroupId;
        this.stateType = stateType;
    }

    /**
     * Can be used with
     * {@code mp.reactive.messaging.incoming.my-channel.commit-strategy=checkpoint}
     * {@code mp.reactive.messaging.incoming.my-channel.checkpoint.state-store=my-store}
     */
    @ApplicationScoped
    @Identifier("my-store")
    public static class Factory implements CheckpointStateStore.Factory {

        @Override
        public CheckpointStateStore create(KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                Class<?> stateType /* if configured, otherwise null */) {
            String consumerGroupId = (String) consumer.configuration().get(ConsumerConfig.GROUP_ID_CONFIG);
            return new MyCheckpointStateStore(consumerGroupId, stateType);
        }
    }

    @Override
    public Uni<Map<TopicPartition, ProcessingState<?>>> fetchProcessingState(Collection<TopicPartition> partitions) {
        // Called on Vert.x event loop
        // Return a Uni completing with the map of topic-partition to processing state
        // The Uni will be subscribed also on Vert.x event loop
        return Uni.createFrom().nullItem();
    }

    @Override
    public Uni<Void> persistProcessingState(Map<TopicPartition, ProcessingState<?>> state) {
        // Called on Vert.x event loop
        // Return a Uni completing with void when the given states are persisted
        // The Uni will be subscribed also on Vert.x event loop
        return Uni.createFrom().voidItem();
    }

    @Override
    public void close() {
        /* Called when channel is closing, no-op by default */
    }
}
