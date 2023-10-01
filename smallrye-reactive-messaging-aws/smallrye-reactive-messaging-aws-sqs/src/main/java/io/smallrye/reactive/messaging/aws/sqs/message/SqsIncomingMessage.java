package io.smallrye.reactive.messaging.aws.sqs.message;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsIncomingMessage extends SqsMessage<Message, SqsIncomingMessageMetadata> {

    private SqsIncomingMessage(Message payload, SqsIncomingMessageMetadata metadata) {
        super(payload, metadata);
    }

    public static <T> SqsIncomingMessage from(Message payload) {
        final SqsIncomingMessageMetadata sqsMetaData = new SqsIncomingMessageMetadata();
        return new SqsIncomingMessage(payload, sqsMetaData);
    }
}
