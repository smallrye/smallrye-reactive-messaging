package consumption;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ConsumptionExamples {

    //tag::message-cs[]
    @Incoming("my-channel")
    public CompletionStage<Void> consumeMessage(Message<Price> message) {
        handle(message.getPayload());
        return message.ack();
    }
    //end::message-cs[]

    //tag::payload-sync[]
    @Incoming("my-channel")
    public void consumePayload(Price payload) {
        // do something
    }
    //end::payload-sync[]

    //tag::payload-cs[]
    @Incoming("my-channel")
    public CompletionStage<Void> consumePayloadCS(Price payload) {
        CompletionStage<Void> cs = handleAsync(payload);
        return cs;
    }
    //end::payload-cs[]

    //tag::payload-uni[]
    @Incoming("my-channel")
    public Uni<Void> consumePayloadUni(Price payload) {
        return Uni.createFrom().item(payload)
            .onItem().invoke(this::handle)
            .onItem().ignore().andContinueWithNull();
    }
    //end::payload-uni[]

    //tag::message-uni[]
    @Incoming("my-channel")
    public Uni<Void> consumeMessageUni(Message<Price> message) {
        return Uni.createFrom().item(message)
            .onItem().invoke(m -> handle(m.getPayload()))
            .onItem().produceCompletionStage(x -> message.ack());
    }
    //end::message-uni[]

    private CompletionStage<Void> handleAsync(Price price) {
        return CompletableFuture.completedFuture(null);
    }

    private Price handle(Price price) {
        return price;
    }

    public static class Price {
        final double price;

        public Price(double p) {
            this.price = p;
        }
    }
}
