package io.smallrye.reactive.messaging.aws.sqs.message;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import io.smallrye.reactive.messaging.aws.sqs.ack.SqsAckHandler;
import software.amazon.awssdk.services.sqs.model.Message;

public class SqsIncomingMessage<T> extends SqsMessage<T, SqsIncomingMessageMetadata> {

    private final SqsAckHandler ackHandler;

    private SqsIncomingMessage(T payload, SqsAckHandler ackHandler, SqsIncomingMessageMetadata metadata) {
        super(payload, metadata);
        this.ackHandler = ackHandler;
    }

    @Override
    public CompletionStage<Void> ack() {
        return ackHandler.handle(this);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return () -> this.ackHandler.handle(this);
    }

    public static SqsIncomingMessage<?> from(Message msg, SqsAckHandler ackHandler) {
        // TODO: deserialize?
        final SqsIncomingMessageMetadata sqsMetaData = new SqsIncomingMessageMetadata(msg);
        return new SqsIncomingMessage<>(msg.body(), ackHandler, sqsMetaData);
    }
}
