package io.smallrye.reactive.messaging.rabbitmq;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/**
 * A bean that can be registered to support consumption of messages from an
 * incoming rabbitmq channel that uses a message converter to transform the body
 * into an integer.
 *
 * Uses the POST_PROCESSING ack strategy.
 */
@ApplicationScoped
public class ConvertedConsumptionWithPostAckBean {

    private final List<Integer> list = new ArrayList<>();

    @Incoming("data")
    @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
    public void process(Integer input) {
        list.add(input);
    }

    public List<Integer> getResults() {
        return list;
    }
}
