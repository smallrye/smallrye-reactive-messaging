package io.smallrye.reactive.messaging.kafka.documentation;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KafkaPriceConsumer {

    private final List<Double> list = new ArrayList<>();

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
        list.add(price);
    }

    public List<Double> list() {
        return list;
    }
}
