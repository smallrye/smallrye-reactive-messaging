package pulsar.inbound;

import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessage;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;
import io.smallrye.reactive.messaging.pulsar.PulsarMessage;

@ApplicationScoped
public class PulsarMessageBatchExample {

    // <code>
    @Incoming("prices")
    public CompletionStage<Void> consumeMessage(PulsarIncomingBatchMessage<Double> messages) {
        for (PulsarMessage<Double> msg : messages) {
            msg.getMetadata(PulsarIncomingMessageMetadata.class).ifPresent(metadata -> {
                String key = metadata.getKey();
                String topic = metadata.getTopicName();
                long timestamp = metadata.getEventTime();
                //... process messages
            });
        }
        // ack will commit the latest offsets (per partition) of the batch.
        return messages.ack();
    }

    @Incoming("prices")
    public void consumeRecords(Messages<Double> messages) {
        for (Message<Double> msg : messages) {
            //... process messages
        }
    }
    // </code>

}
