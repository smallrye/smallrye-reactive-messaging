package examples.snippets;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Subscriber;

@ApplicationScoped
public class ConsumptionExamples {

    @Incoming("data")
    public Subscriber<String> consume() {
        return ReactiveStreams.<String> builder()
                .map(String::toUpperCase)
                .findFirst()
                .build();
    }

    @Incoming("data")
    public Subscriber<Message<String>> consumeMessages() {
        return ReactiveStreams.<Message<String>> builder()
                .findFirst()
                .build();
    }

    @Incoming("data")
    public void consumeOne(String input) {
        //...
    }

    @Incoming("data")
    public CompletionStage<Void> consumeAsync(String input) {
        return CompletableFuture.runAsync(() -> {
            // ...
        });
    }

    @Incoming("data")
    public CompletionStage<Void> consumeMessageAsync(Message<String> input) {
        return CompletableFuture.runAsync(() -> {
            // ...
        });
    }

}
