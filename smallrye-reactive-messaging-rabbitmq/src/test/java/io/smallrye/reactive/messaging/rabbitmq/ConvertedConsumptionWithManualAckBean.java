package io.smallrye.reactive.messaging.rabbitmq;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * A bean that can be registered to support consumption of messages from an
 * incoming rabbitmq channel that uses a message converter to transform the body
 * into an integer.
 *
 * Uses the MANUAL ack strategy.
 */
@ApplicationScoped
public class ConvertedConsumptionWithManualAckBean {

    private final List<Integer> list = new ArrayList<>();

    @Incoming("data")
    @Acknowledgment(Strategy.MANUAL)
    public void process(Integer input) {
        list.add(input + 1);
    }

    public List<Integer> getResults() {
        return list;
    }
}
