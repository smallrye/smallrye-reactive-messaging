package io.smallrye.reactive.messaging.rabbitmq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

/**
 * A bean that can be registered to support consumption of messages from an
 * incoming rabbitmq channel.
 */
@ApplicationScoped
public class ConsumptionBean {

    private final List<Integer> list = new ArrayList<>();

    private final AtomicInteger typeCastCounter = new AtomicInteger();

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(IncomingRabbitMQMessage<String> input) {
        int value = -1;
        try {
            value = Integer.parseInt(input.getPayload());
        } catch (ClassCastException e) {
            typeCastCounter.incrementAndGet();
        }
        return Message.of(value + 1, input::ack);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    public List<Integer> getResults() {
        return list;
    }

    public int getTypeCasts() {
        return typeCastCounter.get();
    }
}
