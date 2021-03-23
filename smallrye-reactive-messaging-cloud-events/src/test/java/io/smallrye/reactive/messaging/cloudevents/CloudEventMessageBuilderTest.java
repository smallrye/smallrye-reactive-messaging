package io.smallrye.reactive.messaging.cloudevents;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.extensions.InMemoryFormat;
import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v1.CloudEventImpl;

class CloudEventMessageBuilderTest {

    @Test
    public void test() {
        // Create from a plain message
        CloudEventMessageBuilder<String> hello = CloudEventMessageBuilder.from(Message.of("hello"))
                .withId("some-id")
                .withSource(URI.create("uri://source1"))
                .withSubject("sub")
                .withType("my-type")
                .withDataContentType("text/plain")
                .withTime(ZonedDateTime.now())
                .withDataschema(URI.create("my://schema"))
                .withExtension(ExtensionFormat.of(InMemoryFormat.of("key", "value", String.class), "k", "v"));

        CloudEventMessage<String> event = hello.build();
        assertThat(event.getPayload()).isEqualTo("hello");
        assertThat(event.getAttributes().getSubject()).contains("sub");
        assertThat(event.getAttributes().getType()).isEqualTo("my-type");
        assertThat(event.getAttributes().getSource()).isEqualTo(URI.create("uri://source1"));

        // Create from cloud event
        CloudEventMessage<String> cem = new DefaultCloudEventMessage<>(CloudEventImpl.build("id", URI.create("uri://source"),
                "type", "text/plain", null, "subject", null, "hello-ce"));
        assertThat(cem.getData()).contains("hello-ce");
        assertThat(cem.getAttributes()).isNotNull();
        assertThat(cem.getExtensions()).isNotNull();
        assertThat(cem.toString()).isNotEmpty();
        event = CloudEventMessageBuilder.from(cem).build();

        assertThat(event.getPayload()).isEqualTo("hello-ce");
        assertThat(event.getAttributes().getId()).isEqualTo("id");
        assertThat(event.getAttributes().getSource()).isEqualTo(URI.create("uri://source"));

        CloudEventMessage<String> message = CloudEventMessageBuilder.from(cem).build("different-data",
                new AttributesImpl("id3", URI.create("uri://source3"), "1.0", "type", "text/plain", null, null, null),
                Collections.emptyList());
        assertThat(message).isNotNull();
    }

}
