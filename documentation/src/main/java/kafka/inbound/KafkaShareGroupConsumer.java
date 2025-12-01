package kafka.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KafkaShareGroupConsumer {

    @Incoming("queue")
    public void consume(double price) {
        // process your price.
    }

}
