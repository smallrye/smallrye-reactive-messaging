package kafka.inbound;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KafkaPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
    }

}
