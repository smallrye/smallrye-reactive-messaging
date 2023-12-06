package io.smallrye.reactive.messaging.aws.sqs.message;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;

public class SqsOutgoingMessage<T> extends SqsMessage<T, SqsOutgoingMessageMetadata> {

    private final Supplier<CompletionStage<Void>> ack;
    private final Function<Throwable, CompletionStage<Void>> nack;

    public SqsOutgoingMessage(
            final T payload,
            final SqsOutgoingMessageMetadata metadata,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        super(payload, metadata);
        this.ack = ack;
        this.nack = nack;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this.ack;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this.nack;
    }

    public static <T> SqsOutgoingMessage<T> from(Message<T> message) {
        return new SqsOutgoingMessage<>(
                message.getPayload(),
                message.getMetadata(SqsOutgoingMessageMetadata.class).orElseGet(SqsOutgoingMessageMetadata::new),
                message.getAck(),
                message.getNack());
    }

}
