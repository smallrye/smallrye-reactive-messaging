package acme;

import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.KafkaMessage;
import io.smallrye.reactive.messaging.kafka.MessageHeaders;

@ApplicationScoped
public class Receiver {

    @Incoming("kafka")
    public CompletionStage<Void> consume(KafkaMessage<String, String> message) {
        String payload = message.getPayload();
        String key = message.getKey();
        MessageHeaders headers = message.getMessageHeaders();
        int partition = message.getPartition();
        long timestamp = message.getTimestamp();
        System.out.println("received: " + payload + " from topic " + message.getTopic());
        return message.ack();
    }

}
