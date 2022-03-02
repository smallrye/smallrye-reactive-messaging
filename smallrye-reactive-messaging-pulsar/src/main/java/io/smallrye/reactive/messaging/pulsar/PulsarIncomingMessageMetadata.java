package io.smallrye.reactive.messaging.pulsar;

import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.api.Message;

class PulsarIncomingMessageMetadata implements PulsarMessageMetadata {

    private final org.apache.pulsar.client.api.Message<?> message;

    PulsarIncomingMessageMetadata(Message<?> message) {
        this.message = message;
    }

    @Override
    public Map<String, String> getProperties() {
        return message.getProperties();
    }

    @Override
    public boolean hasProperty(String name) {
        return message.hasProperty(name);
    }

    @Override
    public String getProperty(String name) {
        return message.getProperty(name);
    }

    public byte[] getData() {
        return message.getData();
    }

    @Override
    public int size() {
        return message.size();
    }

    @Override
    public long getPublishTime() {
        return message.getPublishTime();
    }

    @Override
    public long getEventTime() {
        return message.getEventTime();
    }

    @Override
    public boolean hasKey() {
        return message.hasKey();
    }

    @Override
    public String getKey() {
        return message.getKey();
    }

    @Override
    public boolean hasBase64EncodedKey() {
        return message.hasBase64EncodedKey();
    }

    @Override
    public byte[] getKeyBytes() {
        return message.getKeyBytes();
    }

    @Override
    public boolean hasOrderingKey() {
        return message.hasOrderingKey();
    }

    @Override
    public byte[] getOrderingKey() {
        return message.getOrderingKey();
    }

    @Override
    public String getTopicName() {
        return message.getTopicName();
    }

    @Override
    public int getRedeliveryCount() {
        return message.getRedeliveryCount();
    }

    @Override
    public byte[] getSchemaVersion() {
        return message.getSchemaVersion();
    }

    @Override
    public boolean isReplicated() {
        return message.isReplicated();
    }

    @Override
    public String getReplicatedFrom() {
        return message.getReplicatedFrom();
    }

    @Override
    public boolean hasBrokerPublishTime() {
        return message.hasBrokerPublishTime();
    }

    @Override
    public Optional<Long> getBrokerPublishTime() {
        return message.getBrokerPublishTime();
    }

    @Override
    public boolean hasIndex() {
        return message.hasIndex();
    }

    @Override
    public Optional<Long> getIndex() {
        return message.getIndex();
    }
}
