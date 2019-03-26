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
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class ProducingKafkaMessageBean {

  private AtomicInteger counter = new AtomicInteger();

  @Incoming("data")
  @Outgoing("output-2")
  @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
  public KafkaMessage<String, Integer> process(Message<Integer> input) {
    KafkaMessage<String, Integer> message = KafkaMessage.of(Integer.toString(input.getPayload()), input.getPayload() + 1);
    message.getHeaders().put("hello", "clement").put("count", Integer.toString(counter.incrementAndGet()));
    return message;
  }

  @Outgoing("data")
  public Publisher<Integer> source() {
    return Flowable.range(0, 10);
  }

  @Produces
  public Config myKafkaSinkConfig() {
    String prefix = "mp.messaging.outgoing.output-2.";
    Map<String, Object> config = new HashMap<>();
    config.put(prefix + "type", Kafka.class.getName());
    config.put(prefix + "bootstrap.servers", "localhost:9092");
    config.put(prefix + "key.deserializer", StringDeserializer.class.getName());
    config.put(prefix + "key.serializer", StringSerializer.class.getName());
    config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
    config.put(prefix + "value.serializer", IntegerSerializer.class.getName());
    config.put(prefix + "acks", "1");
    config.put(prefix + "topic", "output-2");

    return new MapBasedConfig(config);
  }

}
