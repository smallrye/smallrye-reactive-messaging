package io.smallrye.reactive.messaging.mqtt;

import io.reactivex.Flowable;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
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
  @Outgoing("sink")
  @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
  public Message<Integer> process(Message<Integer> input) {
    return Message.of(input.getPayload() + 1);
  }

  @Outgoing("data")
  public Publisher<Integer> source() {
    return Flowable.range(0, 10);
  }

  @Produces
  public Config myConfig() {
    String prefix = "mp.messaging.outgoing.sink.";
    Map<String, Object> config = new HashMap<>();
    config.put(prefix + "topic", "sink");
    config.put(prefix + "connector", MqttConnector.CONNECTOR_NAME);
    config.put(prefix + "host", System.getProperty("mqtt-host"));
    config.put(prefix + "port", Integer.valueOf(System.getProperty("mqtt-port")));
    if (System.getProperty("mqtt-user") != null) {
      config.put(prefix + "username", System.getProperty("mqtt-user"));
      config.put(prefix + "password", System.getProperty("mqtt-pwd"));
    }
    return new MapBasedConfig(config);
  }

}
