package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JmsPriceConsumer {

    @Incoming("prices")
    public void consume(double price) {
        // process your price.
    }

}
