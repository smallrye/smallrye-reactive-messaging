package io.smallrye.reactive.messaging.ce;

import static io.smallrye.reactive.messaging.ce.CloudEventMetadata.CE_VERSION_1_0;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.smallrye.reactive.messaging.ce.impl.BaseCloudEventMetadata;

public final class DefaultCloudEventMetadataBuilder<T> {
    private String id;
    private String specVersion = CE_VERSION_1_0;
    private URI source;
    private String type;
    private String dataContentType;
    private URI dataSchema;
    private String subject;
    private ZonedDateTime timestamp;
    private final Map<String, Object> extensions = new HashMap<>();
    private T data;

    public DefaultCloudEventMetadataBuilder() {
        // Do nothing by default
    }

    public DefaultCloudEventMetadataBuilder<T> withId(String id) {
        this.id = id;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withSpecVersion(String specVersion) {
        this.specVersion = specVersion;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withSource(URI source) {
        this.source = source;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withType(String type) {
        this.type = type;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withDataContentType(String dataContentType) {
        this.dataContentType = dataContentType;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withDataSchema(URI dataSchema) {
        this.dataSchema = dataSchema;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withSubject(String subject) {
        this.subject = subject;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withTimestamp(ZonedDateTime timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withExtensions(Map<String, Object> extensions) {
        Objects.requireNonNull(extensions, "The extension map must not be `null`");
        this.extensions.putAll(extensions);
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withExtension(String name, Object value) {
        this.extensions.put(
                Objects.requireNonNull(name, "The attribute name must not be `null`"),
                Objects.requireNonNull(value, "The attribute value must not be `null`"));
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withoutExtension(String name) {
        this.extensions.remove(
                Objects.requireNonNull(name, "The attribute name must not be `null`"));
        return this;
    }

    public DefaultCloudEventMetadataBuilder<T> withData(T data) {
        this.data = data;
        return this;
    }

    public BaseCloudEventMetadata<T> build() {
        return new BaseCloudEventMetadata<>(specVersion, id, source, type, dataContentType, dataSchema, subject,
                timestamp, extensions, data);
    }
}
