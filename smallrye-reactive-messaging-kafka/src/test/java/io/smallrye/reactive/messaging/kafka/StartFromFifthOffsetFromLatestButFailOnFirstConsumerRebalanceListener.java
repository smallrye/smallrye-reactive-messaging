package io.smallrye.reactive.messaging.kafka;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import io.smallrye.mutiny.Uni;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

@ApplicationScoped
@Named("my-group-starting-on-fifth-fail-on-first-attempt")
public class StartFromFifthOffsetFromLatestButFailOnFirstConsumerRebalanceListener
        extends StartFromFifthOffsetFromLatestConsumerRebalanceListener {

    private final AtomicBoolean failOnFirstAttempt = new AtomicBoolean(true);

    @Override
    public Uni<Void> onPartitionsAssigned(KafkaConsumer<?, ?> consumer, Set<TopicPartition> set) {
        // will perform the underlying operation but simulate an error on the first attempt
        return super.onPartitionsAssigned(consumer, set)
                .onItem()
                .transformToUni(a -> {
                    if (!set.isEmpty() && failOnFirstAttempt.getAndSet(false)) {
                        return Uni
                                .createFrom()
                                .failure(new Exception("testing failure"));
                    } else {
                        return Uni
                                .createFrom()
                                .item(a);
                    }
                });
    }
}
