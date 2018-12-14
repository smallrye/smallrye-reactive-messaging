package io.smallrye.reactive.messaging.cloudevents;

import io.cloudevents.CloudEventBuilder;
import io.cloudevents.Extension;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;

public class CloudEventMessageBuilder<T> extends CloudEventBuilder<T> {

  public static <T> CloudEventMessageBuilder<T> from(Message<T> message) {
    CloudEventMessageBuilder<T> builder = new CloudEventMessageBuilder<>();
    builder.data(message.getPayload());
    if (message instanceof CloudEventMessage) {
      CloudEventMessage<T> cem = (CloudEventMessage) message;
      builder
        .id(cem.getId())
        .type(cem.getType())
        .source(cem.getSource())
        .time(cem.getTime().orElseGet(ZonedDateTime::now))
        .contentType(cem.getContentType().orElse(null))
        .schemaURL(cem.getSchemaURL().orElse(null))
        .specVersion(cem.getSpecVersion());
      cem.getExtensions().orElse(Collections.emptyList()).forEach(builder::extension);
    }
    return builder;
  }

  @Override
  public CloudEventMessageBuilder<T> specVersion(String specVersion) {
    super.specVersion(specVersion);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> type(String type) {
    super.type(type);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> source(URI source) {
    super.source(source);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> id(String id) {
    super.id(id);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> time(ZonedDateTime time) {
    super.time(time);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> schemaURL(URI schemaURL) {
    super.schemaURL(schemaURL);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> contentType(String contentType) {
    super.contentType(contentType);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> data(T data) {
    super.data(data);
    return this;
  }

  @Override
  public CloudEventMessageBuilder<T> extension(Extension extension) {
    super.extension(extension);
    return this;
  }

  @Override
  public CloudEventMessage<T> build() {
    return new DefaultCloudEventMessage<>(super.build());
  }
}
