package io.smallrye.reactive.messaging.pulsar;

import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.transaction.Transaction;

/**
 * Pulsar outgoing message as payload
 *
 * @param <T> value type
 */
public class OutgoingMessage<T> {

    private Object value;
    private boolean hasKey;
    private String key;
    private byte[] keyBytes;
    private byte[] orderingKey;
    private Map<String, String> properties;
    private List<String> replicatedClusters;
    private boolean replicationDisabled = false;
    private Long eventTime;
    private Long sequenceId;
    private Long deliverAt;

    private Transaction transaction;

    public static <T> OutgoingMessage<T> of(String key, T value) {
        return new OutgoingMessage<>(key, value);
    }

    public static <T> OutgoingMessage<T> from(Message<T> incoming) {
        OutgoingMessage<T> outgoing = new OutgoingMessage<>(incoming.getValue());
        if (incoming.hasKey()) {
            if (incoming.getKeyBytes() != null) {
                outgoing.withKeyBytes(incoming.getKeyBytes());
            } else {
                outgoing.withKey(incoming.getKey());
            }
        }
        return outgoing;
    }

    public OutgoingMessage(T value) {
        this.value = value;
    }

    public OutgoingMessage(String key, T value) {
        this(value);
        withKey(key);
    }

    public boolean hasKey() {
        return hasKey;
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public OutgoingMessage<T> withKeyBytes(byte[] keyBytes) {
        this.hasKey = keyBytes != null;
        this.keyBytes = keyBytes;
        return this;
    }

    public String getKey() {
        return key;
    }

    public OutgoingMessage<T> withKey(String key) {
        this.hasKey = key != null;
        this.key = key;
        return this;
    }

    public byte[] getOrderingKey() {
        return orderingKey;
    }

    public OutgoingMessage<T> withOrderingKey(byte[] orderingKey) {
        this.orderingKey = orderingKey;
        return this;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public OutgoingMessage<T> withProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    public List<String> getReplicatedClusters() {
        return replicatedClusters;
    }

    public OutgoingMessage<T> withReplicatedClusters(List<String> replicatedClusters) {
        this.replicatedClusters = replicatedClusters;
        return this;
    }

    public boolean getReplicationDisabled() {
        return replicationDisabled;
    }

    public OutgoingMessage<T> withDisabledReplication() {
        this.replicationDisabled = true;
        return this;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public OutgoingMessage<T> withEventTime(long eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    public Long getSequenceId() {
        return sequenceId;
    }

    public OutgoingMessage<T> withSequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    public Long getDeliverAt() {
        return deliverAt;
    }

    public OutgoingMessage<T> withDeliverAt(long deliverAt) {
        this.deliverAt = deliverAt;
        return this;
    }

    public T getValue() {
        return (T) value;
    }

    public <O> OutgoingMessage<O> withValue(O value) {
        this.value = value;
        return new OutgoingMessage<>(value);
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public OutgoingMessage<T> withTransaction(Transaction txn) {
        this.transaction = txn;
        return this;
    }

}
