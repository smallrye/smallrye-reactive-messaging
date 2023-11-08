package connectors.tracing;

import java.util.Map;

public class MyTrace {
    private final String clientId;
    private final String topic;
    private final Map<String, String> messageProperties;

    private MyTrace(String clientId, String topic, Map<String, String> messageProperties) {
        this.clientId = clientId;
        this.topic = topic;
        this.messageProperties = messageProperties;
    }

    public String getClientId() {
        return clientId;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getMessageProperties() {
        return messageProperties;
    }

    public static class Builder {
        private String clientId;
        private String topic;
        private Map<String, String> properties;

        public Builder withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public MyTrace build() {
            return new MyTrace(clientId, topic, properties);
        }
    }
}
