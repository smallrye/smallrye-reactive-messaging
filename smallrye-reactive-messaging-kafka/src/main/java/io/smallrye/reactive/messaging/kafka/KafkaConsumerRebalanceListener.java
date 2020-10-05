package io.smallrye.reactive.messaging.kafka;

import java.util.Set;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

/**
 *
 * When implemented by a managed bean annotated with {@link javax.inject.Named} and
 * configured against an inbound connector will be applied as a consumer re-balance listener
 * to that inbound connector's consumer.
 *
 *
 * To configure which listener you want to use, set the name in the inbound connector's consumer re-balance listener attribute,
 * ex:
 * {@code
 *  mp.messaging.incoming.example.consumer-rebalance- listener.name=ExampleConsumerRebalanceListener
 * }
 * {@code @Named("ExampleConsumerRebalanceListener")}
 *
 * Alternatively, name your listener (using the {@code @Named} annotation) to be the group id used by the connector, ex:
 * {@code
 *  mp.messaging.incoming.example.group.id=my-group
 * }
 * {@code @Named("my-group")}
 *
 * Setting the consumer re-balance listener name takes precedence over using the group id.
 *
 * For more details:
 *
 * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener
 */
public interface KafkaConsumerRebalanceListener {

    /**
     * Called when the consumer is assigned topic partitions
     * This method might be called for each consumer available to the connector
     *
     * The consumer will be paused until the returned {@link Uni}
     * indicates success. On failure will retry using an exponential back off until the consumer can
     * be considered timed-out by Kafka, in which case will resume anyway triggering a new re-balance.
     *
     * @see KafkaConsumer#pause()
     * @see KafkaConsumer#resume()
     *
     * @param consumer underlying consumer
     * @param topicPartitions set of assigned topic partitions
     * @return A {@link Uni} indicating operations complete or failure
     */
    Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions);

    /**
     * Called when the consumer is revoked topic partitions
     * This method might be called for each consumer available to the connector.
     *
     * If the implementation handles its own offsets and commit, it must use the `ignored` commit strategy.
     * In this case, implementation <strong>must</strong> commits the offset of the processed records coming from the
     * revoked partitions in this method.
     *
     * @param consumer underlying consumer
     * @param topicPartitions set of revoked topic partitions
     * @return A {@link Uni} indicating operations complete or failure
     */
    Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions);
}
