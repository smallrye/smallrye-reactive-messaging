package io.smallrye.reactive.messaging.ce.impl;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;

/**
 * Default implementation of the {@link CloudEventMetadata} interface
 *
 * @param <T> the type of data
 */
public class BaseCloudEventMetadata<T> implements CloudEventMetadata<T> {

    protected final String id;
    protected final String specVersion;
    protected final URI source;
    protected final String type;
    protected final String dataContentType;
    protected final URI dataSchema;
    protected final String subject;
    protected final ZonedDateTime timestamp;

    protected final Map<String, Object> extensions;
    protected final T data;

    public BaseCloudEventMetadata(String specVersion, String id, URI source, String type,
            String dataContentType, URI dataSchema, String subject, ZonedDateTime timestamp,
            Map<String, Object> extensions, T data) {

        // Mandatory:
        this.id = id;
        this.specVersion = specVersion;
        this.source = source;
        this.type = type;

        // Optional
        this.dataContentType = dataContentType;
        this.dataSchema = dataSchema;
        this.subject = subject;
        this.timestamp = timestamp;

        // Extensions
        if (extensions == null) {
            this.extensions = Collections.emptyMap();
        } else {
            this.extensions = extensions;
        }

        // Data
        this.data = data;
    }

    public void validate() {
        Objects.requireNonNull(id, "id must not be `null`");
        Objects.requireNonNull(specVersion, "specVersion must not be `null`");
        Objects.requireNonNull(source, "source must not be `null`");
        Objects.requireNonNull(type, "type must not be `null`");
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public URI getSource() {
        return source;
    }

    @Override
    public String getSpecVersion() {
        return specVersion;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public Optional<String> getDataContentType() {
        return Optional.ofNullable(dataContentType);
    }

    @Override
    public Optional<URI> getDataSchema() {
        return Optional.ofNullable(dataSchema);
    }

    @Override
    public Optional<String> getSubject() {
        return Optional.ofNullable(subject);
    }

    @Override
    public Optional<ZonedDateTime> getTimeStamp() {
        return Optional.ofNullable(timestamp);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <A> Optional<A> getExtension(String name) {
        Objects.requireNonNull(name, "The attribute name cannot be `null`");

        switch (name) {
            case CE_ATTRIBUTE_SPEC_VERSION:
                return Optional.of((A) specVersion);
            case CE_ATTRIBUTE_ID:
                return Optional.of((A) id);
            case CE_ATTRIBUTE_SOURCE:
                return Optional.of((A) source);
            case CE_ATTRIBUTE_TYPE:
                return Optional.of((A) this.type);
            case CE_ATTRIBUTE_DATA_CONTENT_TYPE:
                return (Optional<A>) getDataContentType();
            case CE_ATTRIBUTE_DATA_SCHEMA:
                return (Optional<A>) getDataSchema();
            case CE_ATTRIBUTE_SUBJECT:
                return (Optional<A>) getSubject();
            case CE_ATTRIBUTE_TIME:
                return (Optional<A>) getTimeStamp();
            default:
                return (Optional<A>) Optional.ofNullable(extensions.get(name));
        }
    }

    @Override
    public Map<String, Object> getExtensions() {
        if (extensions.isEmpty()) {
            return Collections.emptyMap();
        }
        return new HashMap<>(extensions);
    }

    @Override
    public T getData() {
        return data;
    }

}
