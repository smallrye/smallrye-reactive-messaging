package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
@Identifier("my-group-starting-on-fifth-fail-until-second-rebalance")
public class StartFromFifthOffsetFromLatestButFailUntilSecondRebalanceConsumerRebalanceListener
        extends StartFromFifthOffsetFromLatestConsumerRebalanceListener {
    private final AtomicBoolean failOnFirstAttempt = new AtomicBoolean(true);

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        if (failOnFirstAttempt.getAndSet(false)) {
            throw new IllegalArgumentException("testing failure");
        } else {
            super.onPartitionsAssigned(consumer, partitions);
        }
    }
}
