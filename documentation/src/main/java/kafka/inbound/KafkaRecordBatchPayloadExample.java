package kafka.inbound;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KafkaRecordBatchPayloadExample {

    // tag::code[]
    @Incoming("prices")
    public void consume(List<Double> prices) {
        for (double price : prices) {
            // process price
        }
    }
    // end::code[]

}
