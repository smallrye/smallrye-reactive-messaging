package io.smallrye.reactive.messaging.aws.sqs;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsMessage implements org.eclipse.microprofile.reactive.messaging.Message<String> {

    private final Message message;

    public SqsMessage(Message message) {
        this.message = message;
    }

    @Override
    public String getPayload() {
        return message.body();
    }

    public Message getMessage() {
        return message;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        return this::ack;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return throwable -> CompletableFuture.supplyAsync(null);
    }
}
