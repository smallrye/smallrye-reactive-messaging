package connectors;

public class MyOutgoingMetadata {
    private String topic;
    private String key;

    public static MyOutgoingMetadataBuilder builder() {
        return new MyOutgoingMetadataBuilder();
    }

    public static MyOutgoingMetadataBuilder builder(MyOutgoingMetadata metadata) {
        return new MyOutgoingMetadataBuilder(metadata);
    }

    public MyOutgoingMetadata(String topic, String key) {
        this.topic = topic;
        this.key = key;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public static class MyOutgoingMetadataBuilder {
        private String topic;
        private String key;

        public MyOutgoingMetadataBuilder() {

        }

        public MyOutgoingMetadataBuilder(MyOutgoingMetadata metadata) {
            this.key = metadata.getKey();
            this.topic = metadata.getTopic();
        }

        public MyOutgoingMetadataBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public MyOutgoingMetadataBuilder withKey(String key) {
            this.key = key;
            return this;
        }

        public MyOutgoingMetadata build() {
            return new MyOutgoingMetadata(topic, key);
        }
    }

}
