package io.smallrye.reactive.messaging.pulsar;

import org.eclipse.microprofile.reactive.messaging.Message;

public class PulsarMessage implements Message<byte[]> {

    org.apache.pulsar.client.api.Message<byte[]> delegate;

    @Override
    public byte[] getPayload() {
        return delegate.getData();
    }

    public String getKey() {
        return delegate.getKey();
    }

    public String getTopicName() {
        return delegate.getTopicName();
    }
}
