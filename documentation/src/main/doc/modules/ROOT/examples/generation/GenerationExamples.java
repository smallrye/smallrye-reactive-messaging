package generation;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import messages.MessageExamples;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.reactivestreams.Publisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class GenerationExamples {

    AtomicInteger counter = new AtomicInteger();

    // tag::message-sync[]
    @Outgoing("my-channel")
    public Message<Integer> generateMessagesSynchronously() {
        return Message.of(counter.getAndIncrement());
    }
    // end::message-sync[]

    // tag::message-cs[]
    @Outgoing("my-channel")
    public CompletionStage<Message<Price>> generateMessagesAsCompletionStage() {
        return asyncClient.poll()
            .thenApply(Message::of);
    }
    // end::message-cs[]

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

    // tag::message-uni[]
    @Outgoing("my-channel")
    public Uni<Message<Integer>> generateMessagesAsync() {
        return Uni.createFrom().item(() -> Message.of(counter.getAndIncrement()));
    }
    // end::message-uni[]

    // tag::message-stream[]
    public Publisher<Message<String>> generateMessageStream() {
        Multi<String> multi = reactiveClient.getStream();
        return multi.map(Message::of);
    }
    // end::message-stream[]

    private ReactiveClient<String> reactiveClient = () -> Multi.createFrom().items("a", "b", "c");

    private interface ReactiveClient<T> {
        Multi<T> getStream();
    }

    // tag::payload-sync[]
    @Outgoing("my-channel")
    public Integer generatePayloadsSynchronously() {
        return counter.getAndIncrement();
    }
    // end::payload-sync[]

    // tag::payload-cs[]
    @Outgoing("my-channel")
    public CompletionStage<Price> generatePayloadsAsCompletionStage() {
        return asyncClient.poll();
    }
    // end::payload-cs[]

    // tag::payload-uni[]
    @Outgoing("my-channel")
    public Uni<Integer> generatePayloadsAsync() {
        return Uni.createFrom().item(() -> counter.getAndIncrement());
    }
    // end::payload-uni[]

    // tag::payload-stream[]
    @Outgoing("my-channel")
    public Multi<String> generatePayloadsStream() {
        Multi<String> multi = reactiveClient.getStream();
        return multi;
    }
    // end::payload-stream[]
}
