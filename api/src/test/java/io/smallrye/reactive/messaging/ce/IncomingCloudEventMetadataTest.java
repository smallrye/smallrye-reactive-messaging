package io.smallrye.reactive.messaging.ce;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ce.impl.DefaultIncomingCloudEventMetadata;

public class IncomingCloudEventMetadataTest {

    private final DefaultCloudEventMetadataBuilder<String> builder = new DefaultCloudEventMetadataBuilder<>();

    @Test
    public void testCreation() {
        IncomingCloudEventMetadata<String> event = new DefaultIncomingCloudEventMetadata<>(builder
                .withId("id")
                .withSource(URI.create("test://cloud.event"))
                .withType("type")
                .build());

        assertThat(event.getSubject()).isEmpty();
        assertThat(event.getSpecVersion()).isEqualTo(IncomingCloudEventMetadata.CE_VERSION_1_0);
        assertThat(event.getId()).isEqualTo("id");
        assertThat(event.getData()).isNull();
        assertThat(event.getType()).isEqualTo("type");
        assertThat(event.getSource()).hasHost("cloud.event").hasScheme("test");

        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_SPEC_VERSION))
                .hasValue(IncomingCloudEventMetadata.CE_VERSION_1_0);
        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_ID))
                .hasValue("id");
        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_TYPE))
                .hasValue("type");
        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_SOURCE))
                .hasValue(URI.create("test://cloud.event"));

        event = new DefaultIncomingCloudEventMetadata<>(builder
                .withSpecVersion("v1-test")
                .build());

        assertThat(event.getSubject()).isEmpty();
        assertThat(event.getId()).isEqualTo("id");
        assertThat(event.getSpecVersion()).isEqualTo("v1-test");
        assertThat(event.getData()).isNull();
        assertThat(event.getType()).isEqualTo("type");
        assertThat(event.getSource()).hasHost("cloud.event").hasScheme("test");
    }

    @Test
    public void testWithData() {
        IncomingCloudEventMetadata<String> event = new DefaultIncomingCloudEventMetadata<>(
                builder
                        .withId("id")
                        .withSource(URI.create("test://cloud.event"))
                        .withType("type")
                        .withData("Hello")
                        .build());

        assertThat(event.getData()).isEqualTo("Hello");
    }

    @Test
    public void testExtensionAttribute() {
        IncomingCloudEventMetadata<String> event = new DefaultIncomingCloudEventMetadata<>(
                builder
                        .withId("id")
                        .withSource(URI.create("test://cloud.event"))
                        .withType("type")
                        .withExtension("some-attribute", "some-value")
                        .withExtension("some-int", 10)
                        .withExtension("some-url", URI.create("http://acme.org"))
                        .build());

        assertThat(event.getSubject()).isEmpty();
        assertThat(event.getId()).isEqualTo("id");
        assertThat(event.getData()).isNull();
        assertThat(event.getType()).isEqualTo("type");
        assertThat(event.getSource()).hasHost("cloud.event").hasScheme("test");
        assertThat(event.getExtension("some-attribute")).contains("some-value");
        assertThat(event.getExtension("some-int")).contains(10);
        assertThat(event.getExtension("some-url")).contains(URI.create("http://acme.org"));
        assertThat(event.getExtension("missing")).isEmpty();

        assertThatThrownBy(() -> {
            @SuppressWarnings("unused")
            String e = event.<String> getExtension("some-int").orElse(null);
        }).isInstanceOf(ClassCastException.class);
    }

    @Test
    public void testExtensionAttributes() {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("some-attribute", "some-value");
        attributes.put("some-int", 2);
        attributes.put("some", "entry");

        IncomingCloudEventMetadata<String> event = new DefaultIncomingCloudEventMetadata<>(
                builder
                        .withId("id")
                        .withSource(URI.create("test://cloud.event"))
                        .withType("type")
                        .withExtensions(attributes)
                        .withExtension("some-int", 10)
                        .withExtension("some-url", URI.create("http://acme.org"))
                        .withoutExtension("some")
                        .build());

        assertThat(event.getSubject()).isEmpty();
        assertThat(event.getId()).isEqualTo("id");
        assertThat(event.getData()).isNull();
        assertThat(event.getType()).isEqualTo("type");
        assertThat(event.getSource()).hasHost("cloud.event").hasScheme("test");
        assertThat(event.getExtension("some-attribute")).contains("some-value");
        assertThat(event.getExtension("some-int")).contains(10);
        assertThat(event.getExtension("some-url")).contains(URI.create("http://acme.org"));
        assertThat(event.getExtension("missing")).isEmpty();
        assertThat(event.getExtension("some")).isEmpty();
    }

    @Test
    public void testOptionalAttribute() {
        ZonedDateTime time = ZonedDateTime.now();
        IncomingCloudEventMetadata<String> event = new DefaultIncomingCloudEventMetadata<>(
                builder
                        .withId("id")
                        .withSource(URI.create("test://cloud.event"))
                        .withType("type")
                        .withDataSchema(URI.create("http://schema.org"))
                        .withSubject("subject")
                        .withTimestamp(time)
                        .withDataContentType("application/json")
                        .build());

        assertThat(event.getDataSchema()).hasValue(URI.create("http://schema.org"));
        assertThat(event.getSubject()).hasValue("subject");
        assertThat(event.getTimeStamp()).hasValue(time);
        assertThat(event.getDataContentType()).hasValue("application/json");

        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_DATA_SCHEMA))
                .hasValue(URI.create("http://schema.org"));
        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_SUBJECT))
                .hasValue("subject");
        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_TIME))
                .hasValue(time);
        assertThat(event.getExtension(IncomingCloudEventMetadata.CE_ATTRIBUTE_DATA_CONTENT_TYPE))
                .hasValue("application/json");

        event = new DefaultIncomingCloudEventMetadata<>(new DefaultCloudEventMetadataBuilder<String>()
                .withId("id")
                .withSource(URI.create("test://cloud.event"))
                .withType("type")
                .build());

        assertThat(event.getDataSchema()).isEmpty();
        assertThat(event.getSubject()).isEmpty();
        assertThat(event.getTimeStamp()).isEmpty();
        assertThat(event.getDataContentType()).isEmpty();
    }

}
