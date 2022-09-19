package quickstart;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class ReactiveMessagingExample {

    @Outgoing("source")
    public Multi<String> source() {
        return Multi.createFrom().items("hello", "from", "SmallRye", "reactive", "messaging");
    }

    @Incoming("source")
    @Outgoing("processed-a")
    public String toUpperCase(String payload) {
        return payload.toUpperCase();
    }

    @Incoming("processed-a")
    @Outgoing("processed-b")
    public Multi<String> filter(Multi<String> input) {
        return input.select().where(item -> item.length() > 4);
    }

    @Incoming("processed-b")
    public void sink(String word) {
        System.out.println(">> " + word);
    }

}
