package emitter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;

import io.smallrye.reactive.messaging.annotations.Broadcast;
import messages.MyMetadata;

public class EmitterExamples {

    // <payload>
    @Inject
    @Channel("prices")
    Emitter<Double> emitterForPrices;

    public void send(double d) {
        emitterForPrices.send(d);
    }
    // </payload>

    // <cs>
    public void sendAndAwaitAcknowledgement(double d) {
        CompletionStage<Void> acked = emitterForPrices.send(d);
        // sending a payload returns a CompletionStage completed
        // when the message is acknowledged
        acked.toCompletableFuture().join();
    }
    // </cs>

    // <message>
    public void sendAsMessage(double d) {
        emitterForPrices.send(Message.of(d));
    }
    // </message>

    // <message-ack>
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
    // </message-ack>

    // <message-meta>
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
    // </message-meta>

    // <broadcast>
    @Inject
    @Broadcast
    @Channel("prices")
    Emitter<Double> emitter;

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
    // </broadcast>

    static class OverflowExample {
        // <overflow>

        // Set the max size to 10 and fail if reached
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 10)
        @Inject
        @Channel("channel")
        Emitter<String> emitterWithBuffer;

        // [DANGER ZONE] no limit
        @OnOverflow(OnOverflow.Strategy.UNBOUNDED_BUFFER)
        @Inject
        @Channel("channel")
        Emitter<String> danger;

        // Drop the new messages if the size is reached
        @OnOverflow(OnOverflow.Strategy.DROP)
        @Inject
        @Channel("channel")
        Emitter<String> dropping;

        // Drop the previously sent messages if the size is reached
        @OnOverflow(OnOverflow.Strategy.LATEST)
        @Inject
        @Channel("channel")
        Emitter<String> dropOldMessages;

        // </overflow>
    }

}
