package io.smallrye.reactive.messaging.pulsar.tracing;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.Message;

public class PulsarTrace {
    private final String consumerName;
    private final String topic;
    private final String messageId;
    private final Long sequenceId;
    private final Long uncompressedPayloadSize;
    private final Map<String, String> properties;

    public PulsarTrace(String consumerName,
            String topicName,
            String messageId,
            Long sequenceId,
            Long uncompressedPayloadSize,
            Map<String, String> properties) {
        this.consumerName = consumerName;
        this.topic = topicName;
        this.messageId = messageId;
        this.sequenceId = sequenceId;
        this.uncompressedPayloadSize = uncompressedPayloadSize;
        this.properties = properties;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageId() {
        return messageId;
    }

    public Long getSequenceId() {
        return sequenceId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Long getUncompressedPayloadSize() {
        return uncompressedPayloadSize;
    }

    public static class Builder {
        private String consumerName;
        private String topic;
        private String messageId;
        private Long sequenceId;
        private Long uncompressedPayloadSize;
        private Map<String, String> properties = new HashMap<>();

        public Builder withConsumerName(String consumerName) {
            this.consumerName = consumerName;
            return this;
        }

        public Builder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withMessageId(String messageId) {
            this.messageId = messageId;
            return this;
        }

        public Builder withSequenceId(Long sequenceId) {
            this.sequenceId = sequenceId;
            return this;
        }

        public Builder withUncompressedPayloadSize(Long uncompressedPayloadSize) {
            this.uncompressedPayloadSize = uncompressedPayloadSize;
            return this;
        }

        public Builder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder withMessage(Message<?> message) {
            this.sequenceId = message.getSequenceId();
            this.topic = message.getTopicName();
            this.properties = new HashMap<>(message.getProperties());
            this.messageId = message.getMessageId().toString();
            this.uncompressedPayloadSize = (long) message.size();
            return this;
        }

        public PulsarTrace build() {
            return new PulsarTrace(consumerName, topic, messageId, sequenceId, uncompressedPayloadSize, properties);
        }
    }

}
