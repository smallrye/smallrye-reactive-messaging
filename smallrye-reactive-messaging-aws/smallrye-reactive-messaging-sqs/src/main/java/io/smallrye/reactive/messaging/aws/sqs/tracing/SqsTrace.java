package io.smallrye.reactive.messaging.aws.sqs.tracing;

public class SqsTrace {
    private String queue;
    private String conversationId;
    private String messageId;
    private Long messagePayloadSize;

    public String getQueue() {
        return queue;
    }

    public SqsTrace withQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public String getConversationId() {
        return conversationId;
    }

    public SqsTrace withConversationId(String conversationId) {
        this.conversationId = conversationId;
        return this;
    }

    public String getMessageId() {
        return messageId;
    }

    public SqsTrace withMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public Long getMessagePayloadSize() {
        return messagePayloadSize;
    }

    public SqsTrace withMessagePayloadSize(Long messagePayloadSize) {
        this.messagePayloadSize = messagePayloadSize;
        return this;
    }
}
