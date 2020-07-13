package io.smallrye.reactive.messaging.kafka;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

@ApplicationScoped
@Named("my-group-starting-on-fifth-happy-path")
public class StartFromFifthOffsetFromLatestConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private AtomicInteger rebalanceCount = new AtomicInteger();

    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        rebalanceCount.incrementAndGet();
        return Uni
                .combine()
                .all()
                .unis(set
                        .stream()
                        .map(topicPartition -> consumer.endOffsets(topicPartition)
                                .onItem()
                                .transformToUni(o -> consumer.seek(topicPartition, Math.max(0L, o - 5L))))
                        .collect(Collectors.toList()))
                .combinedWith(a -> null);
    }

    @Override
    public Uni<Void> onPartitionsRevoked(KafkaConsumer<?, ?> consumer, Set<TopicPartition> topicPartitions) {
        return Uni
                .createFrom()
                .nullItem();
    }

    public int getRebalanceCount() {
        return rebalanceCount.get();
    }
}
