package mqtt.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class MqttPriceConsumer {

    @Incoming("prices")
    public void consume(byte[] raw) {
        double price = Double.parseDouble(new String(raw));

        // process your price.
    }

}
