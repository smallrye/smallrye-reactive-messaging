package genericpayload;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.GenericPayload;
import messages.MyMetadata;

@ApplicationScoped
public class GenericPayloadExample {

    // <code>
    @Outgoing("out")
    Multi<GenericPayload<String>> produce() {
        return Multi.createFrom().range(0, 100)
                .map(i -> GenericPayload.of(">> " + i, Metadata.of(new MyMetadata())));
    }
    // </code>

    // <injection>
    @Incoming("in")
    @Outgoing("out")
    GenericPayload<String> process(int payload, MyMetadata metadata) {
        // use the injected metadata
        String id = metadata.getId();
        return GenericPayload.of(">> " + payload + " " + id,
                Metadata.of(metadata, new MyMetadata("Bob", "Alice")));
    }
    // </injection>

}
