package kafka.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.kafka.queues.ShareGroupAcknowledgement;

@ApplicationScoped
public class KafkaShareGroupNackExample {

    // <code>
    @Incoming("queue")
    public CompletionStage<Void> consume(Message<Double> msg) {
        try {
            process(msg.getPayload());
            return msg.ack();
        } catch (TransientException e) {
            // Release for re-delivery
            return msg.nack(e,
                    Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.RELEASE)));
        } catch (Exception e) {
            // Reject permanently
            return msg.nack(e,
                    Metadata.of(ShareGroupAcknowledgement.from(AcknowledgeType.REJECT)));
        }
    }
    // </code>

    private void process(double price) {
    }

    private static class TransientException extends RuntimeException {
    }

}
