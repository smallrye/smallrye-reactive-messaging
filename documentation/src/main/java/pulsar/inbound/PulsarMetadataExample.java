package pulsar.inbound;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.pulsar.PulsarIncomingMessageMetadata;

public class PulsarMetadataExample {

    @SuppressWarnings("unchecked")
    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // <code>
        PulsarIncomingMessageMetadata metadata = incoming.getMetadata(PulsarIncomingMessageMetadata.class).orElse(null);
        if (metadata != null) {
            // The topic
            String topic = metadata.getTopicName();

            // The key
            String key = metadata.getKey();

            // The event time
            long timestamp = metadata.getEventTime();

            // The raw data
            byte[] rawData = metadata.getData();

            // The underlying message
            org.apache.pulsar.client.api.Message<?> message = metadata.getMessage();

            // ...
        }
        // </code>
    }

}
