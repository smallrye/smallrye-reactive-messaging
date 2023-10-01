package io.smallrye.reactive.messaging.aws.sqs.message;

import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.HashMap;
import java.util.Map;

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
