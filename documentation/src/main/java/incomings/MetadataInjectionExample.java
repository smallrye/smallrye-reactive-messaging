package incomings;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Uni;
import jdk.jfr.Period;
import messages.MyMetadata;

public class MetadataInjectionExample {

    //<code>
    @Incoming("in")
    @Outgoing("out")
    public String process(String payload, MyMetadata metadata) {
        // ...
        return payload.toUpperCase();
    }

    @Incoming("in")
    @Outgoing("out")
    public Uni<String> process2(String payload, MyMetadata metadata) {
        // ...
        return Uni.createFrom().item(payload.toUpperCase());
    }

    // ...
    @Incoming("in")
    public void consume(String payload, Optional<MyMetadata> metadata, Period metadata2) {
        // ...
    }

    @Incoming("in")
    public CompletionStage<Void> consume2(String payload, Optional<MyMetadata> metadata, Period metadata2) {
        // ...
        return CompletableFuture.completedFuture(null);
    }
    //</code>

}
