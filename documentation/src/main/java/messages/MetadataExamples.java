package messages;

import java.util.Optional;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

public class MetadataExamples {

    public void extraction() {
        Message<String> message = Message.of("hello");

        // <extraction>
        // retrieve some metadata if present
        Optional<MyMetadata> myMetadata = message.getMetadata(MyMetadata.class);
        message.getMetadata().forEach(metadata -> {
            // iterate through the set of metadata
        });
        // </extraction>

        // <creation>
        MyMetadata metadata = new MyMetadata("author", "me");
        Message<String> msg = Message.of("hello", Metadata.of(metadata));
        // </creation>

    }

}
