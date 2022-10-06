package inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KafkaPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        System.out.println("KafkaPriceConsumer: consume " + price);
    }
}
