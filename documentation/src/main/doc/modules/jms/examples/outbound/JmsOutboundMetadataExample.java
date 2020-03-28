package outbound;

import io.smallrye.reactive.messaging.jms.JmsProperties;
import io.smallrye.reactive.messaging.jms.JmsPropertiesBuilder;
import io.smallrye.reactive.messaging.jms.OutgoingJmsMessageMetadata;
import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

public class JmsOutboundMetadataExample {

    public Message<Double> metadata(Message<Double> incoming) {

        // tag::code[]
        OutgoingJmsMessageMetadata metadata = OutgoingJmsMessageMetadata.builder()
            .withProperties(JmsProperties.builder().with("some-property", "some-value").build())
            .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // end::code[]
    }

}
