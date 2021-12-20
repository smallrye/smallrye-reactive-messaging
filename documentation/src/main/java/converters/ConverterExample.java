package converters;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

public class ConverterExample {

    // <code>
    @Outgoing("persons")
    public Multi<String> source() {
        return Multi.createFrom().items("Neo", "Morpheus", "Trinity");
    }

    // The messages need to be converted as they are emitted as Message<String>
    // and consumed as Message<Person>
    @Incoming("persons")
    public void consume(Person p) {
        // ...
    }
    // </code>

}
