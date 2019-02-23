package io.smallrye.reactive.messaging.kafka;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class ProducingBean {

  @Incoming("data")
  @Outgoing("output")
  @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
  public Message<Integer> process(Message<Integer> input) {
    return Message.of(input.getPayload() + 1);
  }

  @Outgoing("data")
  public Publisher<Integer> source() {
    return Flowable.range(0, 10);
  }

  @Produces
  public Config myKafkaSinkConfig() {
    String prefix = "smallrye.messaging.sink.output.";
    Map<String, String> config = new HashMap<>();
    config.put(prefix + "type", Kafka.class.getName());
    config.put(prefix + "bootstrap.servers", "localhost:9092");
    config.put(prefix + "key.deserializer", StringDeserializer.class.getName());
    config.put(prefix + "key.serializer", StringSerializer.class.getName());
    config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
    config.put(prefix + "value.serializer", IntegerSerializer.class.getName());
    config.put(prefix + "acks", "1");
    config.put(prefix + "topic", "output");

    return new MyKafkaConfig(config);
  }

}
