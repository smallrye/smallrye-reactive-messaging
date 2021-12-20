package consumption;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;

public class ConsumptionExamples {

    //<message-cs>
    @Incoming("my-channel")
    public CompletionStage<Void> consumeMessage(Message<Price> message) {
        handle(message.getPayload());
        return message.ack();
    }
    //</message-cs>

    //<payload-sync>
    @Incoming("my-channel")
    public void consumePayload(Price payload) {
        // do something
    }
    //</payload-sync>

    //<payload-cs>
    @Incoming("my-channel")
    public CompletionStage<Void> consumePayloadCS(Price payload) {
        CompletionStage<Void> cs = handleAsync(payload);
        return cs;
    }
    //</payload-cs>

    //<payload-uni>
    @Incoming("my-channel")
    public Uni<Void> consumePayloadUni(Price payload) {
        return Uni.createFrom().item(payload)
                .onItem().invoke(this::handle)
                .onItem().ignore().andContinueWithNull();
    }
    //</payload-uni>

    //<message-uni>
    @Incoming("my-channel")
    public Uni<Void> consumeMessageUni(Message<Price> message) {
        return Uni.createFrom().item(message)
                .onItem().invoke(m -> handle(m.getPayload()))
                .onItem().transformToUni(x -> Uni.createFrom().completionStage(message.ack()));
    }
    //</message-uni>

    private CompletionStage<Void> handleAsync(Price price) {
        return CompletableFuture.completedFuture(null);
    }

    @SuppressWarnings("UnusedReturnValue")
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
