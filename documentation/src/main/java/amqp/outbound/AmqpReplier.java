package amqp.outbound;

import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class AmqpReplier {

    Random rand = new Random();

    @Incoming("request")
    @Outgoing("reply")
    String handleRequest(String request) {
        return request + "-" + rand.nextInt(100);
    }
}
