package inbound;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MqttPriceConsumer {

    @Incoming("prices")
    public void consume(byte[] raw) {
        double price = Double.parseDouble(new String(raw));

        // process your price.
    }

}
