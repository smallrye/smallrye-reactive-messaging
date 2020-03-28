package inbound;

import io.smallrye.reactive.messaging.jms.IncomingJmsMessageMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;

import javax.jms.Destination;
import java.util.Optional;

public class JmsMetadataExample {

    public void metadata() {
        Message<Double> incoming = Message.of(12.0);
        // tag::code[]
        Optional<IncomingJmsMessageMetadata> metadata = incoming.getMetadata(IncomingJmsMessageMetadata.class);
        metadata.ifPresent(meta -> {
            long expiration = meta.getExpiration();
            Destination destination = meta.getDestination();
            String value = meta.getStringProperty("my-property");
        });
        // end::code[]
    }

}
