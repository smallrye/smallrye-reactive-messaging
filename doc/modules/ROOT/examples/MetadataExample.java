package io.smallrye.reactive.messaging.snippets;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.util.Optional;

public class MetadataExample {

    // tag::metadata[]
    public static class MyMetadata {
        private final String message;

        public MyMetadata(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }

    public void example() {
        Message<String> message = Message.of("hello", Metadata.of(new MyMetadata("some message")));
        Optional<MyMetadata> metadata = message.getMetadata(MyMetadata.class);
    }
    // end::metadata[]


}
