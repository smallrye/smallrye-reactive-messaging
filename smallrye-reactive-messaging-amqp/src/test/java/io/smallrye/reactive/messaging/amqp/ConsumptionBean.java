package io.smallrye.reactive.messaging.amqp;

import io.smallrye.reactive.messaging.annotations.Acknowledgment;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ConsumptionBean {

  private List<Integer> list = new ArrayList<>();

  @Incoming("data")
  @Outgoing("sink")
  @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
  public Message<Integer> process(AmqpMessage<Integer> input) {
    int value = input.getPayload();
    return Message.of(value + 1);
  }

  @Incoming("sink")
  public void sink(int val) {
    list.add(val);
  }

  @Produces
  public Config myConfig() {
    String prefix = "smallrye.messaging.source.data.";
    Map<String, String> config = new HashMap<>();
    config.put(prefix + "address", "data");
    config.put(prefix + "type", Amqp.class.getName());
    config.put(prefix + "host", System.getProperty("amqp-host"));
    config.put(prefix + "port", System.getProperty("amqp-port"));
    if (System.getProperty("amqp-user") != null) {
      config.put(prefix + "username", System.getProperty("amqp-user"));
      config.put(prefix + "password", System.getProperty("amqp-pwd"));
    }
    return new MyConfig(config);
  }


  public List<Integer> getResults() {
    return list;
  }
}
