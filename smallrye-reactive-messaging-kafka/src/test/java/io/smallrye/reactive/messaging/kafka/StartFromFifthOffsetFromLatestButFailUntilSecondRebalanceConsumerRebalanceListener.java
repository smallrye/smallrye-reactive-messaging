package io.smallrye.reactive.messaging.kafka;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

@ApplicationScoped
@Named("my-group-starting-on-fifth-fail-until-second-rebalance")
public class StartFromFifthOffsetFromLatestButFailUntilSecondRebalanceConsumerRebalanceListener
        extends StartFromFifthOffsetFromLatestConsumerRebalanceListener {
    private final AtomicBoolean failOnFirstAttempt = new AtomicBoolean(true);

    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        if (!set.isEmpty() && failOnFirstAttempt.getAndSet(false)) {
            // will perform the underlying operation but simulate an error
            return super.onPartitionsAssigned(consumer, set)
                    .onItem()
                    .failWith(a -> new Exception("testing failure"));
        }
        return super.onPartitionsAssigned(consumer, set);

    }
}
