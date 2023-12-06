package io.smallrye.reactive.messaging.aws.sqs.message;

import static io.smallrye.reactive.messaging.aws.sqs.i18n.SqsMessages.msg;

import java.util.Objects;
import java.util.function.UnaryOperator;

import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsOutgoingMessageMetadata extends SqsMessageMetadata {

    private UnaryOperator<SendMessageRequest.Builder> messageAugmenter = UnaryOperator.identity();

    private UnaryOperator<SendMessageBatchRequestEntry.Builder> messageBatchAugmenter = UnaryOperator.identity();

    public UnaryOperator<SendMessageRequest.Builder> getMessageAugmenter() {
        return messageAugmenter;
    }

    public SqsOutgoingMessageMetadata withMessageAugmenter(
            final UnaryOperator<SendMessageRequest.Builder> messageAugmenter) {
        Objects.requireNonNull(messageAugmenter, msg.isRequired("messageAugmenter"));
        this.messageAugmenter = messageAugmenter;
        return this;
    }

    public UnaryOperator<SendMessageBatchRequestEntry.Builder> getMessageBatchAugmenter() {
        return messageBatchAugmenter;
    }

    public SqsOutgoingMessageMetadata withMessageBatchAugmenter(
            final UnaryOperator<SendMessageBatchRequestEntry.Builder> messageBatchAugmenter) {
        Objects.requireNonNull(messageAugmenter, msg.isRequired("messageBatchAugmenter"));
        this.messageBatchAugmenter = messageBatchAugmenter;
        return this;
    }
}
