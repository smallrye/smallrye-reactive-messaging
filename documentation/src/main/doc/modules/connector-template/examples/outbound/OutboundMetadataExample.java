package outbound;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

public class OutboundMetadataExample {

    public Message<Double> metadata(Message<Double> incoming) {
        // TASK Update the class name and the builder API

        // tag::code[]
        Object metadata = new Object();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // end::code[]
    }

}
