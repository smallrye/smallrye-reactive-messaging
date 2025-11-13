package io.smallrye.reactive.messaging.aws.sqs.tracing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class SqsTrace {
    private final String queue;
    private final String messageId;
    private final Map<String, MessageAttributeValue> attributes;

    public SqsTrace(String queue, Message sqsMessage) {
        this.queue = queue;
        this.messageId = sqsMessage.messageId();
        this.attributes = sqsMessage.hasMessageAttributes() ? sqsMessage.messageAttributes() : new HashMap<>();
    }

    public SqsTrace(String queue, Map<String, MessageAttributeValue> attributes) {
        this.queue = queue;
        this.messageId = null;
        this.attributes = attributes;
    }

    public String getQueue() {
        return queue;
    }

    public List<String> getPropertyNames() {
        return new ArrayList<>(attributes.keySet());
    }

    public String getProperty(final String key) {
        MessageAttributeValue attributeValue = attributes.get(key);
        if (attributeValue == null) {
            return null;
        }
        return attributeValue.stringValue();
    }

    public void setProperty(final String key, final String value) {
        attributes.put(key, MessageAttributeValue.builder()
                .dataType("String")
                .stringValue(value).build());
    }

    public String getMessageId() {
        return messageId;
    }
}
