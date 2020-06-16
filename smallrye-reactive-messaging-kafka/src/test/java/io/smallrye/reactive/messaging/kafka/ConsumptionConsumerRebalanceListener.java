package io.smallrye.reactive.messaging.kafka;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

@ApplicationScoped
@Named("ConsumptionConsumerRebalanceListener")
public class ConsumptionConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private final Map<Integer, TopicPartition> assigned = new ConcurrentHashMap<>();

    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        set.forEach(topicPartition -> this.assigned.put(topicPartition.getPartition(), topicPartition));
        return Uni
                .createFrom()
                .nullItem();
    }

    @Override
    public Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        return Uni
                .createFrom()
                .nullItem();
    }

    public Map<Integer, TopicPartition> getAssigned() {
        return assigned;
    }
}
