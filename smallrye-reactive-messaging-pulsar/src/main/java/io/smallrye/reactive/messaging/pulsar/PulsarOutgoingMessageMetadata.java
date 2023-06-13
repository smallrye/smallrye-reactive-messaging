package io.smallrye.reactive.messaging.pulsar;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.transaction.Transaction;

public class PulsarOutgoingMessageMetadata {

    private final String key;
    private final byte[] keyBytes;
    private final boolean hasKey;
    private final byte[] orderingKey;
    private final Map<String, String> properties;
    private final Long eventTime;
    private final Long sequenceId;
    private final List<String> replicatedClusters;
    private final boolean replicationDisabled;
    private final Long deliverAt;
    private final Transaction transaction;

    public static PulsarOutgoingMessageMetadataBuilder builder() {
        return new PulsarOutgoingMessageMetadataBuilder();
    }

    protected PulsarOutgoingMessageMetadata(String key, byte[] keyBytes, boolean hasKey, byte[] orderingKey,
            Map<String, String> properties, Long eventTime, Long sequenceId, List<String> replicatedClusters,
            boolean replicationDisabled, Long deliverAt, Transaction transaction) {
        this.key = key;
        this.keyBytes = keyBytes;
        this.hasKey = hasKey;
        this.orderingKey = orderingKey;
        this.properties = properties;
        this.eventTime = eventTime;
        this.sequenceId = sequenceId;
        this.replicatedClusters = replicatedClusters;
        this.replicationDisabled = replicationDisabled;
        this.deliverAt = deliverAt;
        this.transaction = transaction;
    }

    public String getKey() {
        return key;
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public boolean hasKey() {
        return hasKey;
    }

    public byte[] getOrderingKey() {
        return orderingKey;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public Long getSequenceId() {
        return sequenceId;
    }

    public List<String> getReplicatedClusters() {
        return replicatedClusters;
    }

    public Boolean getReplicationDisabled() {
        return replicationDisabled;
    }

    public Long getDeliverAt() {
        return deliverAt;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public static class PulsarOutgoingMessageMetadataBuilder {
        private String key;
        private boolean hasKey;
        private byte[] keyBytes;
        private byte[] orderingKey;
        private Map<String, String> properties;
        private Long timestamp;
        private Long sequenceId;
        private List<String> replicatedClusters;
        private boolean replicationDisabled;
        private Long deliverAt;
        private Transaction transaction;

        public PulsarOutgoingMessageMetadataBuilder withKey(String key) {
            this.key = key;
            this.hasKey = true;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withKeyBytes(byte[] keyBytes) {
            this.keyBytes = keyBytes;
            this.hasKey = true;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withOrderingKey(byte[] orderingKey) {
            this.orderingKey = orderingKey;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withEventTime(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withSequenceId(long sequenceId) {
            this.sequenceId = sequenceId;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withReplicatedClusters(List<String> replicatedClusters) {
            this.replicatedClusters = replicatedClusters;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withDisabledReplication() {
            this.replicationDisabled = true;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withDeliverAfter(Duration deliverAfter) {
            this.deliverAt = System.currentTimeMillis() + deliverAfter.toMillis();
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withDeliverAt(long deliverAt) {
            this.deliverAt = deliverAt;
            return this;
        }

        public PulsarOutgoingMessageMetadataBuilder withTransaction(Transaction transaction) {
            this.transaction = transaction;
            return this;
        }

        public PulsarOutgoingMessageMetadata build() {
            return new PulsarOutgoingMessageMetadata(key, keyBytes, hasKey, orderingKey, properties, timestamp,
                    sequenceId, replicatedClusters, replicationDisabled, deliverAt, transaction);
        }
    }
}
