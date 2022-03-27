package io.smallrye.reactive.messaging.ce.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.assertj.core.data.MapEntry;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.DefaultCloudEventMetadataBuilder;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadataBuilder;

class CloudEventMetadataTest {

    private static final URI SOURCE = URI.create("source://my-source");
    private static final URI SCHEMA = URI.create("source://my-schema");

    @Test
    public void testValidation() {
        BaseCloudEventMetadata<String> base = new BaseCloudEventMetadata<>(null, "id", SOURCE,
                "type", null, null, null, null, null, null);
        assertThat(base).isNotNull();
        assertThatThrownBy(base::validate).isInstanceOf(NullPointerException.class).hasMessageContaining("specVersion");

        base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, null, SOURCE,
                "type", null, null, null, null, null, null);
        assertThatThrownBy(base::validate).isInstanceOf(NullPointerException.class).hasMessageContaining("id");

        base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", null,
                "type", null, null, null, null, null, null);
        assertThatThrownBy(base::validate).isInstanceOf(NullPointerException.class).hasMessageContaining("source");

        base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", SOURCE,
                null, null, null, null, null, null, null);
        assertThatThrownBy(base::validate).isInstanceOf(NullPointerException.class).hasMessageContaining("type");
    }

    @Test
    public void testAccessor() {
        BaseCloudEventMetadata<String> base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", SOURCE,
                "type", null, null, null, null, null, null);
        assertThat(base).isNotNull();
        assertThat(base.getId()).isEqualTo("id");
        assertThat(base.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(base.getType()).isEqualTo("type");
        assertThat(base.getSource()).isEqualTo(SOURCE);

        ZonedDateTime now = ZonedDateTime.now();
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("ext", "val");
        base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", SOURCE,
                "type", "application/json", SCHEMA, "subject", now, extensions, "something");
        assertThat(base.getId()).isEqualTo("id");
        assertThat(base.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(base.getType()).isEqualTo("type");
        assertThat(base.getSource()).isEqualTo(SOURCE);
        assertThat(base.getData()).isEqualTo("something");
        assertThat(base.getExtension("ext")).hasValue("val");
        assertThat(base.getExtensions()).containsExactly(MapEntry.entry("ext", "val"));
        assertThat(base.getSubject()).hasValue("subject");
        assertThat(base.getDataSchema()).hasValue(SCHEMA);
        assertThat(base.getTimeStamp()).hasValue(now);
        assertThat(base.getDataContentType()).hasValue("application/json");
    }

    @Test
    void testCreationWithoutExt() {
        BaseCloudEventMetadata<String> base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", SOURCE,
                "type", null, null, null, null, null, null);
        assertThat(base.getExtensions()).isEmpty();
        assertThat(base.getExtension("missing")).isEmpty();
    }

    @Test
    void testIncomingCloudEventMetadata() {
        DefaultIncomingCloudEventMetadata<String> metadata = new DefaultIncomingCloudEventMetadata<>(
                CloudEventMetadata.CE_VERSION_1_0,
                "id", SOURCE, "type", null, null, null, null, null, null);
        metadata.validate();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);

        ZonedDateTime now = ZonedDateTime.now();
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("ext", "val");
        BaseCloudEventMetadata<String> base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", SOURCE,
                "type", "application/json", SCHEMA, "subject", now, extensions, "something");

        metadata = new DefaultIncomingCloudEventMetadata<>(base);
        metadata.validate();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getData()).isEqualTo("something");
        assertThat(metadata.getExtension("ext")).hasValue("val");
        assertThat(metadata.getExtensions()).containsExactly(MapEntry.entry("ext", "val"));
        assertThat(metadata.getSubject()).hasValue("subject");
        assertThat(metadata.getDataSchema()).hasValue(SCHEMA);
        assertThat(metadata.getTimeStamp()).hasValue(now);
        assertThat(metadata.getDataContentType()).hasValue("application/json");
    }

    @Test
    void testOutgoingCloudEventMetadata() {
        DefaultOutgoingCloudEventMetadata<String> metadata = new DefaultOutgoingCloudEventMetadata<>(
                CloudEventMetadata.CE_VERSION_1_0,
                "id", SOURCE, "type", null, null, null, null, null);
        metadata.validate();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);

        ZonedDateTime now = ZonedDateTime.now();
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("ext", "val");
        BaseCloudEventMetadata<String> base = new BaseCloudEventMetadata<>(CloudEventMetadata.CE_VERSION_1_0, "id", SOURCE,
                "type", "application/json", SCHEMA, "subject", now, extensions, "something");

        metadata = new DefaultOutgoingCloudEventMetadata<>(base);
        metadata.validate();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getData()).isNull(); // Outgoing metadata cannot have data, as it will be the message payload.
        assertThat(metadata.getExtension("ext")).hasValue("val");
        assertThat(metadata.getExtensions()).containsExactly(MapEntry.entry("ext", "val"));
        assertThat(metadata.getSubject()).hasValue("subject");
        assertThat(metadata.getDataSchema()).hasValue(SCHEMA);
        assertThat(metadata.getTimeStamp()).hasValue(now);
        assertThat(metadata.getDataContentType()).hasValue("application/json");
    }

    @Test
    void testBaseBuilder() {
        ZonedDateTime now = ZonedDateTime.now();
        DefaultCloudEventMetadataBuilder<String> builder = new DefaultCloudEventMetadataBuilder<>();
        builder.withId("id")
                .withSubject("subject")
                .withType("type")
                .withDataContentType("application/json")
                .withData("something")
                .withTimestamp(now)
                .withExtension("ext", "val")
                .withDataSchema(SCHEMA)
                .withSource(SOURCE);

        BaseCloudEventMetadata<String> metadata = builder.build();
        metadata.validate();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getData()).isEqualTo("something");
        assertThat(metadata.getExtension("ext")).hasValue("val");
        assertThat(metadata.getExtensions()).containsExactly(MapEntry.entry("ext", "val"));
        assertThat(metadata.getSubject()).hasValue("subject");
        assertThat(metadata.getDataSchema()).hasValue(SCHEMA);
        assertThat(metadata.getTimeStamp()).hasValue(now);
        assertThat(metadata.getDataContentType()).hasValue("application/json");

        CloudEventMetadata<String> m = builder
                .withoutExtension("ext")
                .withExtensions(Collections.singletonMap("some", "value"))
                .build();
        assertThat(m.getExtension("ext")).isEmpty();
        assertThat(m.getExtensions()).containsExactly(MapEntry.entry("some", "value"));
    }

    @Test
    void testOutgoingBuilder() {
        ZonedDateTime now = ZonedDateTime.now();
        OutgoingCloudEventMetadataBuilder<String> builder = new OutgoingCloudEventMetadataBuilder<>();
        builder.withId("id")
                .withSubject("subject")
                .withType("type")
                .withDataContentType("application/json")
                .withTimestamp(now)
                .withExtension("ext", "val")
                .withDataSchema(SCHEMA)
                .withSource(SOURCE);

        OutgoingCloudEventMetadata<String> metadata = builder.build();
        assertThat(metadata).isNotNull();
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getId()).isEqualTo("id");
        assertThat(metadata.getSpecVersion()).isEqualTo(CloudEventMetadata.CE_VERSION_1_0);
        assertThat(metadata.getType()).isEqualTo("type");
        assertThat(metadata.getSource()).isEqualTo(SOURCE);
        assertThat(metadata.getData()).isNull();
        assertThat(metadata.getExtension("ext")).hasValue("val");
        assertThat(metadata.getExtensions()).containsExactly(MapEntry.entry("ext", "val"));
        assertThat(metadata.getSubject()).hasValue("subject");
        assertThat(metadata.getDataSchema()).hasValue(SCHEMA);
        assertThat(metadata.getTimeStamp()).hasValue(now);
        assertThat(metadata.getDataContentType()).hasValue("application/json");

        OutgoingCloudEventMetadata<String> m = builder.withoutExtension("ext")
                .withExtensions(Collections.singletonMap("some", "value"))
                .build();
        assertThat(m.getExtension("ext")).isEmpty();
        assertThat(m.getExtensions()).containsExactly(MapEntry.entry("some", "value"));
    }

}
