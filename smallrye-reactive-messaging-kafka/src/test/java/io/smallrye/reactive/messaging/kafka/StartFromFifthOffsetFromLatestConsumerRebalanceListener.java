package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

@ApplicationScoped
@Named("my-group-starting-on-fifth-happy-path")
public class StartFromFifthOffsetFromLatestConsumerRebalanceListener implements KafkaConsumerRebalanceListener {

    private final AtomicInteger rebalanceCount = new AtomicInteger();

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> set) {
        rebalanceCount.incrementAndGet();
        Map<TopicPartition, Long> offsets = consumer.endOffsets(set);
        offsets.forEach((t, o) -> consumer.seek(t, Math.max(0L, o - 5L)));
    }

    public int getRebalanceCount() {
        return rebalanceCount.get();
    }
}
