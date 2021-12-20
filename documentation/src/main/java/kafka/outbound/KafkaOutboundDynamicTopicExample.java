package kafka.outbound;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

public class KafkaOutboundDynamicTopicExample {

    public Message<Double> metadata(Message<Double> incoming) {
        // <code>
        String topicName = selectTopicFromIncommingMessage(incoming);
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String> builder()
                .withTopic(topicName)
                .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // </code>
    }

    private String selectTopicFromIncommingMessage(Message<Double> incoming) {
        return "fake";
    }
}
