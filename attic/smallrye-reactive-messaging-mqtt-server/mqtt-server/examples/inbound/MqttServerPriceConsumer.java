package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MqttServerPriceConsumer {

    @Incoming("prices")
    public void consume(byte[] raw) {
        double price = Double.parseDouble(new String(raw));

        // process your price.
    }

}
