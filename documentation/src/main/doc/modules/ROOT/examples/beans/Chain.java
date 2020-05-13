package beans;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

public class Chain {


    // tag::chain[]
    @Outgoing("source")
    public Multi<String> generate() {
        return Multi.createFrom().items("Hello", "from", "reactive", "messaging");
    }

    @Incoming("source")
    @Outgoing("sink")
    public String process(String in) {
        return in.toUpperCase();
    }

    @Incoming("sink")
    public void consume(String processed) {
        System.out.println(processed);
    }
    // end::chain[]
}
