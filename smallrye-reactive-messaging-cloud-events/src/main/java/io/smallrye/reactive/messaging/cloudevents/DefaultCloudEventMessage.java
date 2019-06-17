package io.smallrye.reactive.messaging.cloudevents;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.cloudevents.CloudEvent;
import io.cloudevents.Extension;

public class DefaultCloudEventMessage<T> implements CloudEventMessage<T> {
    private final CloudEvent<T> delegate;

    public DefaultCloudEventMessage(CloudEvent<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getType() {
        return delegate.getType();
    }

    @Override
    public String getSpecVersion() {
        return delegate.getSpecVersion();
    }

    @Override
    public URI getSource() {
        return delegate.getSource();
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public Optional<ZonedDateTime> getTime() {
        return delegate.getTime();
    }

    @Override
    public Optional<URI> getSchemaURL() {
        return delegate.getSchemaURL();
    }

    @Override
    public Optional<String> getContentType() {
        return delegate.getContentType();
    }

    @Override
    public Optional<T> getData() {
        return delegate.getData();
    }

    @Override
    public Optional<List<Extension>> getExtensions() {
        return delegate.getExtensions();
    }

    @Override
    @JsonIgnore
    public T getPayload() {
        return delegate.getData().orElseThrow(() -> new IllegalArgumentException("Invalid message - no payload"));
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
