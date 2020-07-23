package io.smallrye.reactive.messaging.ce;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Default implementation of the {@link CloudEventMetadata} interface
 *
 * @param <T> the type of data
 */
public class DefaultCloudEventMetadata<T> implements CloudEventMetadata<T> {

    private final String id;
    private final String specVersion;
    private final URI source;
    private final String type;
    private final String dataContentType;
    private final URI dataSchema;
    private final String subject;
    private final ZonedDateTime timestamp;

    private final Map<String, Object> extensions;
    private final T data;

    public DefaultCloudEventMetadata(String id, String specVersion, URI source, String type,
            String dataContentType, URI dataSchema, String subject, ZonedDateTime timestamp,
            Map<String, Object> extensions, T data) {

        // Mandatory:
        this.id = Objects.requireNonNull(id, "id must not be `null`");
        this.specVersion = Objects.requireNonNull(specVersion, "specVersion must not be `null`");
        this.source = Objects.requireNonNull(source, "source must not be `null`");
        this.type = Objects.requireNonNull(type, "type must not be `null`");

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
    public T getData() {
        return data;
    }

}
