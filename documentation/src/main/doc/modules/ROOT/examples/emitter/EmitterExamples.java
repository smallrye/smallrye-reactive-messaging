package emitter;

import io.smallrye.reactive.messaging.annotations.Broadcast;
import messages.MyMetadata;
import org.eclipse.microprofile.reactive.messaging.*;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class EmitterExamples {

    // tag::payload[]
    @Inject @Channel("prices") Emitter<Double> emitterForPrices;

    public void send(double d) {
        emitterForPrices.send(d);
    }
    // end::payload[]

    // tag::cs[]
    public void sendAndAwaitAcknowledgement(double d) {
        CompletionStage<Void> acked = emitterForPrices.send(d);
        // sending a payload returns a CompletionStage completed
        // when the message is acknowledged
        acked.toCompletableFuture().join();
    }
    // end::cs[]

    // tag::message[]
    public void sendAsMessage(double d) {
        emitterForPrices.send(Message.of(d));
    }
    // end::message[]

    // tag::message-ack[]
    public void sendAsMessageWithAck(double d) {
        emitterForPrices.send(Message.of(d, () -> {
                // Called when the message is acknowledged.
                return CompletableFuture.completedFuture(null);
            },
            reason -> {
                // Called when the message is acknowledged negatively.
                return CompletableFuture.completedFuture(null);
            }));
    }
    // end::message-ack[]

    // tag::message-meta[]
    public void sendAsMessageWithAckAndMetadata(double d) {
        MyMetadata metadata = new MyMetadata();
        emitterForPrices.send(Message.of(d, Metadata.of(metadata),
            () -> {
                // Called when the message is acknowledged.
                return CompletableFuture.completedFuture(null);
            },
            reason -> {
                // Called when the message is acknowledged negatively.
                return CompletableFuture.completedFuture(null);
            }));
    }
    // end::message-meta[]

    // tag::broadcast[]
    @Inject
    @Broadcast
    @Channel("prices") Emitter<Double> emitter;

    public void emit(double d) {
        emitter.send(d);
    }

    @Incoming("prices")
    public void handle(double d) {
        // Handle the new price
    }

    @Incoming("prices")
    public void audit(double d) {
        // Audit the price change
    }
    // end::broadcast[]

    static class OverflowExample {
        // tag::overflow[]

        // Set the max size to 10 and fail if reached
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 10)
        @Inject @Channel("channel") Emitter<String> emitterWithBuffer;

        // [DANGER ZONE] no limit
        @OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)
        @Inject @Channel("channel") Emitter<String> danger;

        // Drop the new messages if the size is reached
        @OnOverflow(OnOverflow.Strategy.DROP)
        @Inject @Channel("channel") Emitter<String> dropping;

        // Drop the previously sent messages if the size is reached
        @OnOverflow(OnOverflow.Strategy.LATEST)
        @Inject @Channel("channel") Emitter<String> dropOldMessages;

        // end::overflow[]
    }

}
