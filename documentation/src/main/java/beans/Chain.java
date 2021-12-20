package beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

public class Chain {

    // <chain>
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
    // </chain>
}
