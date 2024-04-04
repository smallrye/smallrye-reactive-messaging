package io.smallrye.reactive.messaging.aws.sqs;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsIncomingMetadata {

    private final Message message;

    public SqsIncomingMetadata(Message message) {
        this.message = message;
    }

    public Message getMessage() {
        return message;
    }
}
