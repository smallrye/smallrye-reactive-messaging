package io.smallrye.reactive.messaging.aws.sns;

import java.util.Map;

import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

/**
 * Metadata for the SNS outbound message.
 */
public class SnsOutboundMetadata {

    private final String groupId;
    private final String deduplicationId;
    private final Map<String, MessageAttributeValue> messageAttributes;
    private final String topicArn;
    private final String messageStructure;
    private final String emailSubject;
    private final String smsPhoneNumber;

    public static SnsOutboundMetadata.Builder builder() {
        return new Builder();
    }

    public SnsOutboundMetadata(
            final String groupId,
            final String deduplicationId,
            final Map<String, MessageAttributeValue> messageAttributes,
            final String topicArn,
            final String messageStructure,
            final String emailSubject,
            final String smsPhoneNumber) {
        this.groupId = groupId;
        this.deduplicationId = deduplicationId;
        this.messageAttributes = messageAttributes;
        this.topicArn = topicArn;
        this.messageStructure = messageStructure;
        this.emailSubject = emailSubject;
        this.smsPhoneNumber = smsPhoneNumber;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getDeduplicationId() {
        return deduplicationId;
    }

    public Map<String, MessageAttributeValue> getMessageAttributes() {
        return messageAttributes;
    }

    public String getTopicArn() {
        return topicArn;
    }

    public String getMessageStructure() {
        return messageStructure;
    }

    public String getEmailSubject() {
        return emailSubject;
    }

    public String getSmsPhoneNumber() {
        return smsPhoneNumber;
    }

    public static class Builder {

        private String groupId;
        private String deduplicationId;
        private Map<String, MessageAttributeValue> messageAttributes;
        private String topicArn;
        private String messageStructure;
        private String emailSubject;
        private String smsPhoneNumber;

        public Builder groupId(final String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder deduplicationId(final String deduplicationId) {
            this.deduplicationId = deduplicationId;
            return this;
        }

        public Builder messageAttributes(final Map<String, MessageAttributeValue> messageAttributes) {
            this.messageAttributes = messageAttributes;
            return this;
        }

        public Builder topicArn(final String topicArn) {
            this.topicArn = topicArn;
            return this;
        }

        public Builder messageStructure(final String messageStructure) {
            this.messageStructure = messageStructure;
            return this;
        }

        public Builder emailSubject(final String subject) {
            this.emailSubject = subject;
            return this;
        }

        public Builder smsPhoneNumber(final String phoneNumber) {
            this.smsPhoneNumber = phoneNumber;
            return this;
        }

        public SnsOutboundMetadata build() {
            return new SnsOutboundMetadata(groupId, deduplicationId, messageAttributes, topicArn,
                    messageStructure, emailSubject, smsPhoneNumber);
        }
    }
}
