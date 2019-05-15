package acme;

import io.smallrye.reactive.messaging.mqtt.MqttMessage;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class Receiver {

  @Incoming("my-topic")
  public CompletionStage<Void> consume(MqttMessage message) {
    String payload = message.getPayload().toString();
    System.out.println("received: " + payload + " from topic " + message.getTopic());
    return message.ack();
  }

}

