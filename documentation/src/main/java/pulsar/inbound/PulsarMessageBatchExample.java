package pulsar.inbound;

import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.TracingMetadata;
import io.smallrye.reactive.messaging.pulsar.PulsarIncomingBatchMessageMetadata;

@ApplicationScoped
public class PulsarMessageBatchExample {

    // <code>
    @Incoming("prices")
    public CompletionStage<Void> consumeMessage(Message<List<Double>> messages) {
        messages.getMetadata(PulsarIncomingBatchMessageMetadata.class).ifPresent(metadata -> {
            for (org.apache.pulsar.client.api.Message<Object> message : metadata.getMessages()) {
                String key = message.getKey();
                String topic = message.getTopicName();
                long timestamp = message.getEventTime();
                //... process messages
            }
        });
        // ack will commit the latest offsets (per partition) of the batch.
        return messages.ack();
    }

    @Incoming("prices")
    public void consumeMessages(org.apache.pulsar.client.api.Messages<Double> messages) {
        for (org.apache.pulsar.client.api.Message<Double> msg : messages) {
            //... process messages
        }
    }
    // </code>

    // <batch>
    @Incoming("prices")
    public void consumeMessages(org.apache.pulsar.client.api.Messages<Double> messages,
            PulsarIncomingBatchMessageMetadata metadata) {
        for (org.apache.pulsar.client.api.Message<Double> message : messages) {
            TracingMetadata tracing = metadata.getMetadataForMessage(message, TracingMetadata.class);
            if (tracing != null) {
                tracing.getCurrentContext().makeCurrent();
            }
            //... process messages
        }
    }
    // </batch>

}
