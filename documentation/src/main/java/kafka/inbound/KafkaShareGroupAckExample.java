package kafka.inbound;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.kafka.queues.ShareGroupAcknowledgement;

@ApplicationScoped
public class KafkaShareGroupAckExample {

    // <code>
    @Incoming("queue")
    public void consume(double price, ShareGroupAcknowledgement ack) {
        if (price < 0) {
            ack.reject(); // Permanently reject the record
        } else if (needsRetry(price)) {
            ack.release(); // Release for re-delivery to another consumer
        } else {
            ack.accept(); // Mark as successfully processed
        }
    }
    // </code>

    private boolean needsRetry(double price) {
        return false;
    }

}
