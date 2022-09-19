package io.smallrye.reactive.messaging.kafka;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBean {

    private final List<Integer> list = new CopyOnWriteArrayList<>();
    private final List<KafkaRecord<String, Integer>> kafka = new CopyOnWriteArrayList<>();

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(KafkaRecord<String, Integer> input) {
        kafka.add(input);
        return input.withPayload(input.getPayload() + 1);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    public List<Integer> getResults() {
        return list;
    }

    public List<KafkaRecord<String, Integer>> getKafkaMessages() {
        return kafka;
    }
}
