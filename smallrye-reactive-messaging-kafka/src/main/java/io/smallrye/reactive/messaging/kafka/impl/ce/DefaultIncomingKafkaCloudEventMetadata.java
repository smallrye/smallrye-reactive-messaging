package io.smallrye.reactive.messaging.kafka.impl.ce;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Optional;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata;

public class DefaultIncomingKafkaCloudEventMetadata<K, T> implements IncomingKafkaCloudEventMetadata<K, T> {

    private final CloudEventMetadata<T> delegate;

    public DefaultIncomingKafkaCloudEventMetadata(CloudEventMetadata<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public URI getSource() {
        return delegate.getSource();
    }

    @Override
    public String getSpecVersion() {
        return delegate.getSpecVersion();
    }

    @Override
    public String getType() {
        return delegate.getType();
    }

    @Override
    public Optional<String> getDataContentType() {
        return delegate.getDataContentType();
    }

    @Override
    public Optional<URI> getDataSchema() {
        return delegate.getDataSchema();
    }

    @Override
    public Optional<String> getSubject() {
        return delegate.getSubject();
    }

    @Override
    public Optional<ZonedDateTime> getTimeStamp() {
        return delegate.getTimeStamp();
    }

    @Override
    public <A> Optional<A> getExtension(String name) {
        return delegate.getExtension(name);
    }

    @Override
    public T getData() {
        return delegate.getData();
    }
}
