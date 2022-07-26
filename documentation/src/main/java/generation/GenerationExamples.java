package generation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

public class GenerationExamples {

    AtomicInteger counter = new AtomicInteger();

    // <message-sync>
    @Outgoing("my-channel")
    public Message<Integer> generateMessagesSynchronously() {
        return Message.of(counter.getAndIncrement());
    }
    // </message-sync>

    // <message-cs>
    @Outgoing("my-channel")
    public CompletionStage<Message<Price>> generateMessagesAsCompletionStage() {
        return asyncClient.poll()
                .thenApply(Message::of);
    }
    // </message-cs>

    public static class Price {
        final double price;

        public Price(double p) {
            this.price = p;
        }
    }

    private Pollable<Price> asyncClient = () -> CompletableFuture.completedFuture(new Price(10.0));

    private interface Pollable<T> {
        CompletionStage<T> poll();
    }

    // <message-uni>
    @Outgoing("my-channel")
    public Uni<Message<Integer>> generateMessagesAsync() {
        return Uni.createFrom().item(() -> Message.of(counter.getAndIncrement()));
    }
    // </message-uni>

    // <message-stream>
    public Flow.Publisher<Message<String>> generateMessageStream() {
        Multi<String> multi = reactiveClient.getStream();
        return multi.map(Message::of);
    }
    // </message-stream>

    private ReactiveClient<String> reactiveClient = () -> Multi.createFrom().items("a", "b", "c");

    private interface ReactiveClient<T> {
        Multi<T> getStream();
    }

    // <payload-sync>
    @Outgoing("my-channel")
    public Integer generatePayloadsSynchronously() {
        return counter.getAndIncrement();
    }
    // </payload-sync>

    // <payload-cs>
    @Outgoing("my-channel")
    public CompletionStage<Price> generatePayloadsAsCompletionStage() {
        return asyncClient.poll();
    }
    // </payload-cs>

    // <payload-uni>
    @Outgoing("my-channel")
    public Uni<Integer> generatePayloadsAsync() {
        return Uni.createFrom().item(() -> counter.getAndIncrement());
    }
    // </payload-uni>

    // <payload-stream>
    @Outgoing("my-channel")
    public Multi<String> generatePayloadsStream() {
        Multi<String> multi = reactiveClient.getStream();
        return multi;
    }
    // </payload-stream>
}
