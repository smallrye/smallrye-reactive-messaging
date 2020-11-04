package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

@ApplicationScoped
@Named("ConsumptionConsumerRebalanceListener")
public class ConsumptionConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private final Map<Integer, TopicPartition> assigned = new ConcurrentHashMap<>();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        partitions.forEach(topicPartition -> this.assigned.put(topicPartition.partition(), topicPartition));
    }

    public Map<Integer, TopicPartition> getAssigned() {
        return assigned;
    }
}
