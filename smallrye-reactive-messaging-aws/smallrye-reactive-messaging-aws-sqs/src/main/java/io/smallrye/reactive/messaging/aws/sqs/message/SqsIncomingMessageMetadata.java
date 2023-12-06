package io.smallrye.reactive.messaging.aws.sqs.message;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsIncomingMessageMetadata extends SqsMessageMetadata {

    private final Message awsMessage;

    public SqsIncomingMessageMetadata(Message awsMessage) {
        this.awsMessage = awsMessage;
    }

    public Message getAwsMessage() {
        return awsMessage;
    }
}
