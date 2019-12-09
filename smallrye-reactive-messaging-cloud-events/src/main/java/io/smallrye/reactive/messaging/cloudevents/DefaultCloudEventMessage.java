package io.smallrye.reactive.messaging.cloudevents;

import java.util.Map;
import java.util.Optional;

import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v1.CloudEventImpl;

public class DefaultCloudEventMessage<T> implements CloudEventMessage<T> {
    private final CloudEventImpl<T> delegate;

    public DefaultCloudEventMessage(CloudEventImpl<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public AttributesImpl getAttributes() {
        return delegate.getAttributes();
    }

    @Override
    public Optional<T> getData() {
        return delegate.getData();
    }

    @Override
    public Map<String, Object> getExtensions() {
        return delegate.getExtensions();
    }

    @Override
    public T getPayload() {
        return delegate.getData().orElseThrow(() -> new IllegalArgumentException("Invalid message - no payload"));
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
