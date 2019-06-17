package io.smallrye.reactive.messaging.amqp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class ConsumptionBean {

    private List<Integer> list = new ArrayList<>();

    @Incoming("data")
    @Outgoing("sink")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(AmqpMessage<Integer> input) {
        int value = input.getPayload();
        return Message.of(value + 1, input::ack);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    @Produces
    public Config myConfig() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "address", "data");
        config.put(prefix + "connector", AmqpConnector.CONNECTOR_NAME);
        config.put(prefix + "host", System.getProperty("amqp-host"));
        config.put(prefix + "port", Integer.valueOf(System.getProperty("amqp-port")));
        if (System.getProperty("amqp-user") != null) {
            config.put(prefix + "username", System.getProperty("amqp-user"));
            config.put(prefix + "password", System.getProperty("amqp-pwd"));
        }
        return new MapBasedConfig(config);
    }

    public List<Integer> getResults() {
        return list;
    }
}
