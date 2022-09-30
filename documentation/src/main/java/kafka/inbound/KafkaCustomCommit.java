package kafka.inbound;

import java.util.Collection;
import java.util.function.BiConsumer;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaConnectorIncomingConfiguration;
import io.smallrye.reactive.messaging.kafka.KafkaConsumer;
import io.smallrye.reactive.messaging.kafka.commit.KafkaCommitHandler;
import io.vertx.mutiny.core.Vertx;

public class KafkaCustomCommit implements KafkaCommitHandler {

    @Override
    public <K, V> Uni<Void> handle(IncomingKafkaRecord<K, V> record) {
        // called on message ack
        return Uni.createFrom().voidItem();
    }

    @Override
    public <K, V> Uni<IncomingKafkaRecord<K, V>> received(IncomingKafkaRecord<K, V> record) {
        // called before message processing
        return Uni.createFrom().item(record);
    }

    @Override
    public void terminate(boolean graceful) {
        // called on channel shutdown
    }

    @Override
    public void partitionsAssigned(Collection<TopicPartition> partitions) {
        // called on partitions assignment
    }

    @Override
    public void partitionsRevoked(Collection<TopicPartition> partitions) {
        // called on partitions revoked
    }

    @ApplicationScoped
    @Identifier("custom")
    public static class Factory implements KafkaCommitHandler.Factory {

        @Override
        public KafkaCommitHandler create(KafkaConnectorIncomingConfiguration config,
                Vertx vertx,
                KafkaConsumer<?, ?> consumer,
                BiConsumer<Throwable, Boolean> reportFailure) {
            return new KafkaCustomCommit(/* ... */);
        }
    }

}
