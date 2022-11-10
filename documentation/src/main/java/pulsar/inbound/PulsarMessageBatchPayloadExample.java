package pulsar.inbound;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class PulsarMessageBatchPayloadExample {

    // <code>
    @Incoming("prices")
    public void consume(List<Double> prices) {
        for (double price : prices) {
            // process price
        }
    }
    // </code>

}
