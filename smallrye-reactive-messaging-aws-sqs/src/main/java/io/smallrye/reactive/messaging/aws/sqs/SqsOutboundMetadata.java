package io.smallrye.reactive.messaging.aws.sqs;

import java.util.Map;

import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

/**
 * Metadata for the SQS outbound message.
 */
public class SqsOutboundMetadata {

    private final String groupId;
    private final String deduplicationId;
    private final Integer delaySeconds;
    private final Map<String, MessageAttributeValue> messageAttributes;
    private final String queueUrl;

    public static SqsOutboundMetadata.Builder builder() {
        return new Builder();
    }

    public SqsOutboundMetadata(String groupId, String deduplicationId, Integer delaySeconds,
            Map<String, MessageAttributeValue> messageAttributes, String queueUrl) {
        this.groupId = groupId;
        this.deduplicationId = deduplicationId;
        this.delaySeconds = delaySeconds;
        this.messageAttributes = messageAttributes;
        this.queueUrl = queueUrl;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getDeduplicationId() {
        return deduplicationId;
    }

    public Integer getDelaySeconds() {
        return delaySeconds;
    }

    public Map<String, MessageAttributeValue> getMessageAttributes() {
        return messageAttributes;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    public static class Builder {

        private String messageBody;
        private String groupId;
        private String deduplicationId;
        private Integer delaySeconds;
        private Map<String, MessageAttributeValue> messageAttributes;
        private String queueUrl;

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder deduplicationId(String deduplicationId) {
            this.deduplicationId = deduplicationId;
            return this;
        }

        public Builder delaySeconds(Integer delaySeconds) {
            this.delaySeconds = delaySeconds;
            return this;
        }

        public Builder messageAttributes(Map<String, MessageAttributeValue> messageAttributes) {
            this.messageAttributes = messageAttributes;
            return this;
        }

        public Builder queueUrl(String queueUrl) {
            this.queueUrl = queueUrl;
            return this;
        }

        public SqsOutboundMetadata build() {
            return new SqsOutboundMetadata(groupId, deduplicationId, delaySeconds, messageAttributes, queueUrl);
        }
    }

}
