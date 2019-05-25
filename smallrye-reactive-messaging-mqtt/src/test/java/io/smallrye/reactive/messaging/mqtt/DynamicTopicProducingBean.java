package io.smallrye.reactive.messaging.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.reactivex.Flowable;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class DynamicTopicProducingBean {

  private List<String> topics = new ArrayList<>(10);

  @Incoming("data")
  @Outgoing("sink")
  @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
  public MqttMessage<String> process(Message<Integer> input) {
    String topic = "T" + input.getPayload();
    topics.add(topic);
    System.out.println("Sending on topic: " + topic);
    return MqttMessage.of( topic, input.getPayload().toString(), MqttQoS.AT_LEAST_ONCE, false);
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

  public List<String> getTopics() {
    return topics;
  }
}
