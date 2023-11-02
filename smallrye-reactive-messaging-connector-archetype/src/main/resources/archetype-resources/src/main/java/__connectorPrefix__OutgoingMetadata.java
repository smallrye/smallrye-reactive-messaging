package ${package};

public class ${connectorPrefix}OutgoingMetadata {
    private String topic;
    private String key;

    public static ${connectorPrefix}OutgoingMetadataBuilder builder() {
        return new ${connectorPrefix}OutgoingMetadataBuilder();
    }

    public static ${connectorPrefix}OutgoingMetadataBuilder builder(${connectorPrefix}OutgoingMetadata metadata) {
        return new ${connectorPrefix}OutgoingMetadataBuilder(metadata);
    }

    public ${connectorPrefix}OutgoingMetadata(String topic, String key) {
        this.topic = topic;
        this.key = key;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public static class ${connectorPrefix}OutgoingMetadataBuilder {
        private String topic;
        private String key;

        public ${connectorPrefix}OutgoingMetadataBuilder() {

        }

        public ${connectorPrefix}OutgoingMetadataBuilder(${connectorPrefix}OutgoingMetadata metadata) {
            this.key = metadata.getKey();
            this.topic = metadata.getTopic();
        }

        public ${connectorPrefix}OutgoingMetadataBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public ${connectorPrefix}OutgoingMetadataBuilder withKey(String key) {
            this.key = key;
            return this;
        }

        public ${connectorPrefix}OutgoingMetadata build() {
            return new ${connectorPrefix}OutgoingMetadata(topic, key);
        }
    }

}
