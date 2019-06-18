package io.smallrye.reactive.messaging.kafka;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

@ApplicationScoped
public class ProducingBean {

    @Incoming("data")
    @Outgoing("output")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Message<Integer> process(Message<Integer> input) {
        return Message.of(input.getPayload() + 1, input::ack);
    }

    @Outgoing("data")
    public Publisher<Integer> source() {
        return Flowable.range(0, 10);
    }

    @Produces
    public Config myKafkaSinkConfig() {
        String prefix = "mp.messaging.outgoing.output.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", IntegerSerializer.class.getName());

        return new MapBasedConfig(config);
    }

    @Produces
    @ConfigProperty(name = "kafka.bootstrap.servers")
    public String boot() {
        return "localhost:9092";
    }

}
