package io.smallrye.reactive.messaging.jms;

import java.time.Duration;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class ProducerBean {

    @Outgoing("queue-one")
    public Multi<Integer> producer() {
        return Multi.createFrom().ticks().every(Duration.ofMillis(10))
                .on().overflow().buffer(10)
                .map(Long::intValue)
                .transform().byTakingFirstItems(10);
    }

}
