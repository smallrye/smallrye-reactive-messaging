package io.smallrye.reactive.messaging.mqtt;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBean {

    private final List<Integer> list = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sink")
    public Message<Integer> process(MqttMessage<byte[]> input) {
        String s = new String(input.getPayload());
        return Message.of(Integer.parseInt(s) + 1);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    public List<Integer> getResults() {
        return list;
    }
}
