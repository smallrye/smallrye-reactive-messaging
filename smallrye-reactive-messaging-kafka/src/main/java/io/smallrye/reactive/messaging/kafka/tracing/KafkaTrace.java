package io.smallrye.reactive.messaging.kafka.tracing;

import org.apache.kafka.common.header.Headers;

public class KafkaTrace {
    private final String groupId;
    private final String clientId;
    private final int partition;
    private final String topic;
    private final long offset;
    private final Headers headers;

    private KafkaTrace(final String groupId, final String clientId, final int partition, final String topic,
            final long offset, final Headers headers) {
        this.groupId = groupId;
        this.clientId = clientId;
        this.partition = partition;
        this.topic = topic;
        this.offset = offset;
        this.headers = headers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getClientId() {
        return clientId;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public Headers getHeaders() {
        return headers;
    }

    public long getOffset() {
        return offset;
    }

    public static class Builder {
        private String groupId;
        private String clientId;
        private int partition;
        private String topic;
        private long offset;
        private Headers headers;

        public Builder withGroupId(final String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder withClientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withPartition(final int partition) {
            this.partition = partition;
            return this;
        }

        public Builder withTopic(final String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withOffset(final long offset) {
            this.offset = offset;
            return this;
        }

        public Builder withHeaders(final Headers headers) {
            this.headers = headers;
            return this;
        }

        public KafkaTrace build() {
            return new KafkaTrace(groupId, clientId, partition, topic, offset, headers);
        }
    }
}
