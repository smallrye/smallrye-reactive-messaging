package io.smallrye.reactive.messaging.aws.sqs.message;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsIncomingMessage<T> extends SqsMessage<T, SqsIncomingMessageMetadata> {

    private SqsIncomingMessage(T payload, SqsIncomingMessageMetadata metadata) {
        super(payload, metadata);
    }

    public static SqsIncomingMessage<String> from(Message msg) {
        // TODO: deserialize
        final SqsIncomingMessageMetadata sqsMetaData = new SqsIncomingMessageMetadata(msg);
        return new SqsIncomingMessage<>(msg.body(), sqsMetaData);
    }
}
