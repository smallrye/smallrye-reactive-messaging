package io.smallrye.reactive.messaging.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
    public Message<Integer> process(KafkaMessage<String, Integer> input) {
        return Message.of(input.getPayload() + 1, input::ack);
    }

    @Incoming("sink")
    public void sink(int val) {
        list.add(val);
    }

    @Produces
    public Config myKafkaSourceConfig() {
        String prefix = "mp.messaging.incoming.data.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "bootstrap.servers", "localhost:9092");
        config.put(prefix + "group.id", "my-group");
        config.put(prefix + "key.deserializer", StringDeserializer.class.getName());
        config.put(prefix + "key.serializer", StringSerializer.class.getName());
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "value.serializer", IntegerSerializer.class.getName());
        config.put(prefix + "enable.auto.commit", "false");
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "topic", "data");

        return new MapBasedConfig(config);
    }

    public List<Integer> getResults() {
        return list;
    }
}
