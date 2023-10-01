package io.smallrye.reactive.messaging.aws.sqs.message;

public abstract class SqsMessageMetadata {

    private String queue;
    private String conversationId;

    /**
     * Get the name of the queue
     *
     * @return the queue name
     */
    public String getQueue() {
        return queue;
    }

    public SqsMessageMetadata withQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public String getConversationId() {
        return conversationId;
    }

    public SqsMessageMetadata withConversationId(String conversationId) {
        this.conversationId = conversationId;
        return this;
    }
}
