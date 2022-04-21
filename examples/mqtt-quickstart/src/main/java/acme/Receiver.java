package acme;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.mqtt.MqttMessage;

@ApplicationScoped
public class Receiver {

    @Incoming("my-topic")
    public CompletionStage<Void> consume(MqttMessage<byte[]> message) {
        String payload = new String(message.getPayload());
        System.out.println("received: " + payload + " from topic " + message.getTopic());
        return message.ack();
    }

}
