package io.smallrye.reactive.messaging.cloudevents;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collection;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.fun.EventBuilder;
import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;

public class CloudEventMessageBuilder<T> implements EventBuilder<T, AttributesImpl> {

    private final CloudEventBuilder<T> builder;

    public static <T> CloudEventMessageBuilder<T> from(Message<T> message) {
        CloudEventMessageBuilder<T> builder = new CloudEventMessageBuilder<>();
        builder.withData(message.getPayload());
        if (message instanceof CloudEventMessage) {
            CloudEventMessage<T> cem = (CloudEventMessage<T>) message;

            // Copy the attributes:
            builder.withId(cem.getAttributes().getId());
            builder.withType(cem.getAttributes().getType());
            builder.withSource(cem.getAttributes().getSource());
            cem.getAttributes().getDatacontenttype().ifPresent(builder::withDataContentType);
            cem.getAttributes().getDataschema().ifPresent(builder::withDataschema);
            cem.getAttributes().getSubject().ifPresent(builder::withSubject);
            cem.getAttributes().getTime().ifPresent(builder::withTime);
        }
        return builder;
    }

    public CloudEventMessageBuilder() {
        builder = CloudEventBuilder.builder();
    }

    public CloudEventMessageBuilder<T> withId(String id) {
        builder.withId(id);
        return this;
    }

    public CloudEventMessageBuilder<T> withSource(URI source) {
        builder.withSource(source);
        return this;
    }

    public CloudEventMessageBuilder<T> withType(String type) {
        builder.withType(type);
        return this;
    }

    public CloudEventMessageBuilder<T> withDataschema(URI dataschema) {
        builder.withDataschema(dataschema);
        return this;
    }

    public CloudEventMessageBuilder<T> withDataContentType(String datacontenttype) {
        builder.withDataContentType(datacontenttype);
        return this;
    }

    public CloudEventMessageBuilder<T> withSubject(String subject) {
        builder.withSubject(subject);
        return this;
    }

    public CloudEventMessageBuilder<T> withTime(ZonedDateTime time) {
        builder.withTime(time);
        return this;
    }

    public CloudEventMessageBuilder<T> withData(T data) {
        builder.withData(data);
        return this;
    }

    public CloudEventMessageBuilder<T> withExtension(ExtensionFormat extension) {
        builder.withExtension(extension);
        return this;
    }

    public CloudEventMessage<T> build() {
        return new DefaultCloudEventMessage<>(builder.build());
    }

    @Override
    public CloudEventMessage<T> build(T data, AttributesImpl attributes,
            Collection<ExtensionFormat> extensions) {
        CloudEventBuilder<T> builder = CloudEventBuilder.<T> builder()
                .withId(attributes.getId())
                .withSource(attributes.getSource())
                .withType(attributes.getType());

        attributes.getTime().ifPresent(builder::withTime);
        attributes.getDataschema().ifPresent(builder::withDataschema);
        attributes.getDatacontenttype().ifPresent(builder::withDataContentType);
        attributes.getSubject().ifPresent(builder::withSubject);
        extensions.forEach(builder::withExtension);

        CloudEventImpl<T> event = builder
                .withData(data)
                .build();

        return new DefaultCloudEventMessage<>(event);
    }
}
