package kafka.outbound;

import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class KafkaReplier {

    Random rand = new Random();

    @Incoming("request")
    @Outgoing("reply")
    int handleRequest(String request) {
        return rand.nextInt(100);
    }
}
