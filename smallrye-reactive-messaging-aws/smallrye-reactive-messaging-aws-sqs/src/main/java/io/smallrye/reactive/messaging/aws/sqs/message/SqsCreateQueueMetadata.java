package io.smallrye.reactive.messaging.aws.sqs.message;

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

public class SqsCreateQueueMetadata {

    private Map<QueueAttributeName, String> attributes = new HashMap<>();
    private Map<String, String> tags = new HashMap<>();

    public Map<QueueAttributeName, String> getAttributes() {
        return attributes;
    }

    public SqsCreateQueueMetadata withAttributes(Map<QueueAttributeName, String> attributes) {
        this.attributes = attributes;
        return this;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public SqsCreateQueueMetadata withTags(Map<String, String> tags) {
        this.tags = tags;
        return this;
    }
}
