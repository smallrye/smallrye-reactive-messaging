package io.smallrye.reactive.messaging.jms.tracing;

import java.util.HashMap;
import java.util.Map;

public class JmsTrace {
    private final String groupId;
    private final String clientId;
    private final String queue;
    private final Map<String, Object> messageProperties;

    private JmsTrace(final String groupId, final String clientId, final String queue, Map<String, Object> messageProperties) {
        this.groupId = groupId;
        this.clientId = clientId;
        this.queue = queue;
        this.messageProperties = messageProperties;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getClientId() {
        return clientId;
    }

    public String getQueue() {
        return queue;
    }

    public Map<String, Object> getMessageProperties() {
        return messageProperties;
    }

    public static class Builder {
        private String groupId;
        private String clientId;
        private String queue;
        private Map<String, Object> properties;

        public Builder withGroupId(final String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder withClientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withQueue(final String queue) {
            this.queue = queue;
            return this;
        }

        public Builder withProperties(Map<String, Object> properties) {
            this.properties = new HashMap<>(properties);
            return this;
        }

        public JmsTrace build() {
            return new JmsTrace(groupId, clientId, queue, properties);
        }
    }
}
