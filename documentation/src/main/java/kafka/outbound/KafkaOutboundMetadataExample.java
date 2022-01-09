package kafka.outbound;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

public class KafkaOutboundMetadataExample {

    public Message<Double> metadata(Message<Double> incoming) {
        // <code>
        // Creates an OutgoingKafkaRecordMetadata
        // The type parameter is the type of the record's key
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String> builder()
                .withKey("my-key")
                .withHeaders(new RecordHeaders().add("my-header", "value".getBytes()))
                .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // </code>
    }

}
