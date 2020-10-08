package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;

@ApplicationScoped
@Named("my-group-starting-on-fifth-fail-on-first-attempt")
public class StartFromFifthOffsetFromLatestButFailOnFirstConsumerRebalanceListener
        extends StartFromFifthOffsetFromLatestConsumerRebalanceListener {

    private final AtomicBoolean failOnFirstAttempt = new AtomicBoolean(true);

    @Override public void onPartitionsAssigned(Consumer<?, ?> consumer,
        Collection<org.apache.kafka.common.TopicPartition> partitions) {
        super.onPartitionsAssigned(consumer, partitions);
        if (! partitions.isEmpty()  && failOnFirstAttempt.getAndSet(false)) {
            throw new IllegalArgumentException("testing failure");
        }
    }
}
