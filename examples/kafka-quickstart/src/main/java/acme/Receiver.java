package acme;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@ApplicationScoped
public class Receiver {

    @Incoming("kafka")
    public CompletionStage<Void> consume(KafkaRecord<String, String> message) {
        String payload = message.getPayload();
        String key = message.getKey();
        Headers headers = message.getHeaders();
        int partition = message.getPartition();
        Instant timestamp = message.getTimestamp();
        System.out.println("received: " + payload + " from topic " + message.getTopic());
        return message.ack();
    }

}
