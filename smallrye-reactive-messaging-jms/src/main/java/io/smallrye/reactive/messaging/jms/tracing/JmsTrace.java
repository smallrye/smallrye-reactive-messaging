package io.smallrye.reactive.messaging.jms.tracing;

import java.util.HashMap;
import java.util.Map;

public class JmsTrace {
    private final String queue;
    private final Map<String, Object> messageProperties;

    private JmsTrace(final String queue, Map<String, Object> messageProperties) {
        this.queue = queue;
        this.messageProperties = messageProperties;
    }

    public String getQueue() {
        return queue;
    }

    public Map<String, Object> getMessageProperties() {
        return messageProperties;
    }

    public static class Builder {
        private String queue;
        private Map<String, Object> properties;

        public Builder withQueue(final String queue) {
            this.queue = queue;
            return this;
        }

        public Builder withProperties(Map<String, Object> properties) {
            this.properties = new HashMap<>(properties);
            return this;
        }

        public JmsTrace build() {
            return new JmsTrace(queue, properties);
        }
    }
}
