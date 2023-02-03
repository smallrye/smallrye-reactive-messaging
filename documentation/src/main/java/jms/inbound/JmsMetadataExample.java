package jms.inbound;

import java.util.Optional;

import jakarta.jms.Destination;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.jms.IncomingJmsMessageMetadata;

public class JmsMetadataExample {

    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // <code>
        Optional<IncomingJmsMessageMetadata> metadata = incoming.getMetadata(IncomingJmsMessageMetadata.class);
        metadata.ifPresent(meta -> {
            long expiration = meta.getExpiration();
            Destination destination = meta.getDestination();
            String value = meta.getStringProperty("my-property");
        });
        // </code>
    }

}
