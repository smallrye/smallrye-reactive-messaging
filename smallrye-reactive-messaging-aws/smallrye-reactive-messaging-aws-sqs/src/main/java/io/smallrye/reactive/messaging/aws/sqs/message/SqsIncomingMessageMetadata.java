package io.smallrye.reactive.messaging.aws.sqs.message;

import software.amazon.awssdk.services.sqs.model.Message;

public class SqsIncomingMessageMetadata extends SqsMessageMetadata {

    private Message msg;

    public SqsIncomingMessageMetadata(Message msg) {
        this.msg = msg;
    }

    public Message getMsg() {
        return msg;
    }
}
