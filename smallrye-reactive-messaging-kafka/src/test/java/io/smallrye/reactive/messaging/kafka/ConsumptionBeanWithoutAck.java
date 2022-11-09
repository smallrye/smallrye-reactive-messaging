package io.smallrye.reactive.messaging.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class ConsumptionBeanWithoutAck {

    private final List<Integer> list = Collections.synchronizedList(new ArrayList<>());

    @Incoming("data")
    @Acknowledgment(Acknowledgment.Strategy.NONE)
    public CompletionStage<Void> sink(KafkaRecord<String, Integer> input) {
        list.add(input.getPayload() + 1);
        return CompletableFuture.completedFuture(null);
    }

    public List<Integer> getResults() {
        return list;
    }
}
