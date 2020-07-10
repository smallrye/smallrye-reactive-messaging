package inbound;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaConsumerRebalanceListener;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@ApplicationScoped
@Named("rebalanced-example.rebalancer")
public class KafkaRebalancedConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private static final Logger LOGGER = Logger.getLogger(KafkaRebalancedConsumerRebalanceListener.class.getName());

    /**
     * When receiving a list of partitions will search for the earliest offset within 10 minutes
     * and seek the consumer to it.
     *
     * @param consumer        underlying consumer
     * @param topicPartitions set of assigned topic partitions
     * @return A {@link Uni} indicating operations complete or failure
     */
    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        long now = System.currentTimeMillis();
        long shouldStartAt = now - 600_000L; //10 minute ago

        return Uni
            .combine()
            .all()
            .unis(topicPartitions
                .stream()
                .map(topicPartition -> {
                    LOGGER.info("Assigned " + topicPartition);
                    return consumer.offsetsForTimes(topicPartition, shouldStartAt)
                        .onItem()
                        .invoke(o -> LOGGER.info("Seeking to " + o))
                        .onItem()
                        .transformToUni(o -> consumer
                            .seek(topicPartition, o == null ? 0L : o.getOffset())
                            .onItem()
                            .invoke(v -> LOGGER.info("Seeked to " + o))
                        );
                })
                .collect(Collectors.toList()))
            .combinedWith(a -> null);
    }

    @Override
    public Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        return Uni
            .createFrom()
            .nullItem();
    }
}
