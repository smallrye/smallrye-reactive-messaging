package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import io.smallrye.common.annotation.Identifier;

@ApplicationScoped
@Identifier("my-group-starting-on-fifth-fail-on-first-attempt")
public class StartFromFifthOffsetFromLatestButFailOnFirstConsumerRebalanceListener
        extends StartFromFifthOffsetFromLatestConsumerRebalanceListener {

    private final AtomicBoolean failOnFirstAttempt = new AtomicBoolean(true);

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        super.onPartitionsAssigned(consumer, partitions);
        if (!partitions.isEmpty() && failOnFirstAttempt.getAndSet(false)) {
            throw new IllegalArgumentException("testing failure");
        }
    }
}
