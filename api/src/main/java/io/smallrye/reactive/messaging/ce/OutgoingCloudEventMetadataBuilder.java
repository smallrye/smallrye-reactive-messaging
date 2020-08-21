package io.smallrye.reactive.messaging.ce;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;

import io.smallrye.reactive.messaging.ce.impl.DefaultOutgoingCloudEventMetadata;

public final class OutgoingCloudEventMetadataBuilder<T> {

    private final DefaultCloudEventMetadataBuilder<T> builder = new DefaultCloudEventMetadataBuilder<>();

    /**
     * The Cloud Event Id.
     * We store it to inject a random UUID if not set before the construction.
     */
    private String id;

    public OutgoingCloudEventMetadataBuilder() {
        // Do nothing by default
    }

    public OutgoingCloudEventMetadataBuilder(OutgoingCloudEventMetadata<T> existing) {
        builder.withSpecVersion(existing.getSpecVersion());
        this.id = existing.getId();
        builder.withId(existing.getId());
        builder.withSource(existing.getSource());
        builder.withType(existing.getType());
        builder.withData(existing.getData());

        builder.withExtensions(existing.getExtensions());

        builder.withTimestamp(existing.getTimeStamp().orElse(null));
        builder.withSubject(existing.getSubject().orElse(null));
        builder.withDataSchema(existing.getDataSchema().orElse(null));
        builder.withDataContentType(existing.getDataContentType().orElse(null));
    }

    public OutgoingCloudEventMetadataBuilder<T> withId(String id) {
        this.id = id;
        builder.withId(id);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withSpecVersion(String specVersion) {
        builder.withSpecVersion(specVersion);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withSource(URI source) {
        builder.withSource(source);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withType(String type) {
        builder.withType(type);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withDataContentType(String dataContentType) {
        builder.withDataContentType(dataContentType);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withDataSchema(URI dataSchema) {
        builder.withDataSchema(dataSchema);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withSubject(String subject) {
        builder.withSubject(subject);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withTimestamp(ZonedDateTime timestamp) {
        builder.withTimestamp(timestamp);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withExtensions(Map<String, Object> extensions) {
        builder.withExtensions(extensions);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withExtension(String name, Object value) {
        builder.withExtension(name, value);
        return this;
    }

    public OutgoingCloudEventMetadataBuilder<T> withoutExtension(String name) {
        builder.withoutExtension(name);
        return this;
    }

    public OutgoingCloudEventMetadata<T> build() {
        if (id == null) {
            builder.withId(UUID.randomUUID().toString());
        }
        return new DefaultOutgoingCloudEventMetadata<>(builder.build());
    }
}
