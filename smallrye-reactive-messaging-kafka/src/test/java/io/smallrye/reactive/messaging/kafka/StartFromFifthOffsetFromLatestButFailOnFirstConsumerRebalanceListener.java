package io.smallrye.reactive.messaging.kafka;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

@ApplicationScoped
@Named("my-group-starting-on-fifth-fail-on-first-attempt")
public class StartFromFifthOffsetFromLatestButFailOnFirstConsumerRebalanceListener
        extends StartFromFifthOffsetFromLatestConsumerRebalanceListener {

    private final AtomicBoolean failOnFirstAttempt = new AtomicBoolean(true);

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> set) {
        // will perform the underlying operation but simulate an error on the first attempt
        super.onPartitionsAssigned(consumer, set);

        if (!set.isEmpty() && failOnFirstAttempt.getAndSet(false)) {
            throw new IllegalStateException("testing failure");
        }
    }
}
