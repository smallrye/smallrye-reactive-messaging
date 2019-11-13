package io.smallrye.reactive.messaging.kafka;

import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBean {

    private List<Integer> list = new ArrayList<>();
    private List<KafkaMessage<String, Integer>> kafka = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(KafkaMessage<String, Integer> input) {
        kafka.add(input);
        return Message.of(input.getPayload() + 1, input::ack);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    public List<Integer> getResults() {
        return list;
    }

    public List<KafkaMessage<String, Integer>> getKafkaMessages() {
        return kafka;
    }
}
