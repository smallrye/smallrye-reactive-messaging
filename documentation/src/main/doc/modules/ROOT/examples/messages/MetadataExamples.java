package messages;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.util.Optional;

public class MetadataExamples {


    public void extraction() {
        Message<String> message = Message.of("hello");

        // tag::extraction[]
        // retrieve some metadata if present
        Optional<MyMetadata> myMetadata = message.getMetadata(MyMetadata.class);
        message.getMetadata().forEach(metadata -> {
            // iterate through the set of metadata
        });
        // end::extraction[]

        // tag::creation[]
        MyMetadata metadata = new MyMetadata("author", "me");
        Message<String> msg = Message.of("hello", Metadata.of(metadata));
        // end::creation[]

    }


}
