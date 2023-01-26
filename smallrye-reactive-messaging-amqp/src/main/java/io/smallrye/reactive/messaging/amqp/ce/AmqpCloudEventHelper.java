package io.smallrye.reactive.messaging.amqp.ce;

import static io.smallrye.reactive.messaging.ce.CloudEventMetadata.*;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

import java.net.URI;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

import org.apache.qpid.proton.amqp.messaging.Section;

import io.smallrye.reactive.messaging.amqp.AmqpConnectorOutgoingConfiguration;
import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.DefaultCloudEventMetadataBuilder;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.impl.BaseCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.impl.DefaultIncomingCloudEventMetadata;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.amqp.AmqpMessage;
import io.vertx.mutiny.amqp.AmqpMessageBuilder;

public class AmqpCloudEventHelper {

    public static final String CE_CONTENT_TYPE_PREFIX = "application/cloudevents";
    public static final String CE_HEADER_PREFIX = "cloudEvents:";
    public static final String STRUCTURED_CONTENT_TYPE = CE_CONTENT_TYPE_PREFIX + "+json; charset=UTF-8";

    public static final String AMQP_HEADER_FOR_SPEC_VERSION = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SPEC_VERSION;
    public static final String AMQP_HEADER_FOR_TYPE = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_TYPE;
    public static final String AMQP_HEADER_FOR_SOURCE = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SOURCE;
    public static final String AMQP_HEADER_FOR_ID = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_ID;

    public static final String AMQP_HEADER_FOR_SCHEMA = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_DATA_SCHEMA;
    public static final String AMQP_HEADER_FOR_CONTENT_TYPE = CE_HEADER_PREFIX
            + CloudEventMetadata.CE_ATTRIBUTE_DATA_CONTENT_TYPE;
    public static final String AMQP_HEADER_FOR_SUBJECT = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SUBJECT;
    public static final String AMQP_HEADER_FOR_TIME = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_TIME;

    // TODO Should be replaced with DateTimeFormatter.ISO_OFFSET_DATE_TIME, there for read retro-compatibility
    public static final DateTimeFormatter RFC3339_DATE_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendZoneOrOffsetId()
            .toFormatter();

    private AmqpCloudEventHelper() {
        // avoid direct instantiation
    }

    public static <T> IncomingCloudEventMetadata<T> createFromStructuredCloudEvent(
            io.vertx.amqp.AmqpMessage message) {
        DefaultCloudEventMetadataBuilder<T> builder = new DefaultCloudEventMetadataBuilder<>();

        JsonObject content;

        Section body = message.unwrap().getBody();
        if (body.getType() == Section.SectionType.AmqpValue) {
            // String value
            content = new JsonObject(message.bodyAsString());
        } else if (body.getType() == Section.SectionType.Data) {
            // Byte[]
            content = message.bodyAsBinary().toJsonObject();
        } else {
            throw new IllegalArgumentException(
                    "Invalid value type. Structured Cloud Event can only be created from String, JsonObject and byte[]");
        }

        // Required
        builder.withSpecVersion(content.getString(CloudEventMetadata.CE_ATTRIBUTE_SPEC_VERSION));
        builder.withId(content.getString(CloudEventMetadata.CE_ATTRIBUTE_ID));
        String source = content.getString(CloudEventMetadata.CE_ATTRIBUTE_SOURCE);
        if (source == null) {
            throw new IllegalArgumentException(
                    "The JSON value must contain the " + CloudEventMetadata.CE_ATTRIBUTE_SOURCE + " attribute");
        }
        builder.withSource(URI.create(source));
        builder.withType(content.getString(CloudEventMetadata.CE_ATTRIBUTE_TYPE));

        // Optional
        String ct = content.getString(CloudEventMetadata.CE_ATTRIBUTE_DATA_CONTENT_TYPE);
        if (ct != null) {
            builder.withDataContentType(ct);
        }

        String schema = content.getString(CloudEventMetadata.CE_ATTRIBUTE_DATA_SCHEMA);
        if (schema != null) {
            builder.withDataSchema(URI.create(schema));
        }

        String subject = content.getString(CloudEventMetadata.CE_ATTRIBUTE_SUBJECT);
        if (subject != null) {
            builder.withSubject(subject);
        }

        String time = content.getString(CloudEventMetadata.CE_ATTRIBUTE_TIME);
        if (time != null) {
            builder.withTimestamp(ZonedDateTime.parse(time, RFC3339_DATE_FORMAT));
        }

        // Data
        Object data = content.getValue("data");
        //noinspection unchecked
        builder
                .withData((T) data);

        BaseCloudEventMetadata<T> cloudEventMetadata = builder.build();
        cloudEventMetadata.validate();
        return new DefaultIncomingCloudEventMetadata<>(cloudEventMetadata);
    }

    public static <T> IncomingCloudEventMetadata<T> createFromBinaryCloudEvent(
            io.vertx.amqp.AmqpMessage message, io.smallrye.reactive.messaging.amqp.AmqpMessage<T> parent) {
        DefaultCloudEventMetadataBuilder<T> builder = new DefaultCloudEventMetadataBuilder<>();

        // Create a copy as we are going to remove the entries.
        JsonObject applicationProperties = message.applicationProperties().copy();

        // Required
        builder.withSpecVersion(applicationProperties.getString(AMQP_HEADER_FOR_SPEC_VERSION));
        builder.withId(applicationProperties.getString(AMQP_HEADER_FOR_ID));
        String source = applicationProperties.getString(AMQP_HEADER_FOR_SOURCE);
        if (source == null) {
            throw new IllegalArgumentException(
                    "The Kafka record must contain the " + AMQP_HEADER_FOR_SOURCE + " header");
        }
        builder.withSource(URI.create(source));
        builder.withType(applicationProperties.getString(AMQP_HEADER_FOR_TYPE));

        // Optional
        // Rules 3.1.1 - Set datacontenttype to the record's content type header
        String ct = message.contentType();
        if (ct != null) {
            builder.withDataContentType(ct);
        }

        String schema = applicationProperties.getString(AMQP_HEADER_FOR_SCHEMA);
        if (schema != null) {
            builder.withDataSchema(URI.create(schema));
        }

        String subject = applicationProperties.getString(AMQP_HEADER_FOR_SUBJECT);
        if (subject != null) {
            builder.withSubject(subject);
        }

        String time = applicationProperties.getString(AMQP_HEADER_FOR_TIME);
        if (time != null) {
            ZonedDateTime parse = ZonedDateTime.parse(time, RFC3339_DATE_FORMAT);
            builder.withTimestamp(parse);
        }

        applicationProperties.remove(AMQP_HEADER_FOR_SPEC_VERSION);
        applicationProperties.remove(AMQP_HEADER_FOR_ID);
        applicationProperties.remove(AMQP_HEADER_FOR_SOURCE);
        applicationProperties.remove(AMQP_HEADER_FOR_TYPE);
        applicationProperties.remove(AMQP_HEADER_FOR_SCHEMA);
        applicationProperties.remove(AMQP_HEADER_FOR_SUBJECT);
        applicationProperties.remove(AMQP_HEADER_FOR_TIME);

        applicationProperties.forEach(entry -> {
            if (entry.getKey().startsWith(CE_HEADER_PREFIX)) {
                String key = entry.getKey().substring(CE_HEADER_PREFIX.length());
                builder.withExtension(key, entry.getValue());
            }
        });

        // Data
        builder
                .withData(parent.getPayload());

        BaseCloudEventMetadata<T> cloudEventMetadata = builder.build();
        return new DefaultIncomingCloudEventMetadata<>(cloudEventMetadata);
    }

    public static AmqpMessage createBinaryCloudEventMessage(
            io.vertx.mutiny.amqp.AmqpMessage message,
            OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {

        if (ceMetadata == null) {
            ceMetadata = OutgoingCloudEventMetadata.builder().build();
        }
        Optional<String> subject = getSubject(ceMetadata, configuration);
        Optional<String> contentType = getDataContentType(ceMetadata, configuration);
        Optional<URI> schema = getDataSchema(ceMetadata, configuration);

        AmqpMessageBuilder builder = AmqpMessage.create(message);

        // Add the Cloud Event header - prefixed with cloudEvents: (rules 3.1.3.1)
        // Mandatory headers
        JsonObject app = new JsonObject();
        app.put(AMQP_HEADER_FOR_SPEC_VERSION, ceMetadata.getSpecVersion());
        app.put(AMQP_HEADER_FOR_ID, ceMetadata.getId());
        String type = getType(ceMetadata, configuration);
        app.put(AMQP_HEADER_FOR_TYPE, type);
        String source = getSource(ceMetadata, configuration);
        app.put(AMQP_HEADER_FOR_SOURCE, source);

        // Optional attribute
        subject.ifPresent(
                s -> app.put(AMQP_HEADER_FOR_SUBJECT, s));
        if (contentType.isPresent()) {
            // Rules 3.1.1 - in binary mode, the content-type header must be mapped to the datacontenttype attribute.
            app.put(AMQP_HEADER_FOR_CONTENT_TYPE, contentType.get());
            builder.contentType(contentType.get());
        } else if (message.contentType() != null) {
            // Rules 3.1.1 - in binary mode, the content-type header must be mapped to the datacontenttype attribute.
            app.put(AMQP_HEADER_FOR_CONTENT_TYPE, message.contentType());
        }
        schema.ifPresent(
                s -> app.put(AMQP_HEADER_FOR_SCHEMA, s.toString()));

        Optional<ZonedDateTime> ts = ceMetadata.getTimeStamp();
        if (ts.isPresent()) {
            ZonedDateTime time = ts.get();
            app.put(AMQP_HEADER_FOR_TIME, ISO_OFFSET_DATE_TIME.format(time));
        } else if (configuration.getCloudEventsInsertTimestamp()) {
            ZonedDateTime now = ZonedDateTime.now();
            app.put(AMQP_HEADER_FOR_TIME, ISO_OFFSET_DATE_TIME.format(now));
        }

        // Extensions
        ceMetadata.getExtensions().forEach((k, v) -> {
            if (v != null) {
                app.put(CE_HEADER_PREFIX + k, v);
            }
        });

        if (message.applicationProperties() != null) {
            builder.applicationProperties(app.mergeIn(message.applicationProperties()));
        } else {
            builder.applicationProperties(app);
        }
        return builder.build();
    }

    private static String getSource(OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {
        String source = ceMetadata.getSource() != null ? ceMetadata.getSource().toString() : null;
        if (source == null) {
            source = configuration.getCloudEventsSource().orElseThrow(() -> new IllegalArgumentException(
                    "Cannot build the Cloud Event Record - source is not set"));
        }
        return source;
    }

    private static String getType(OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {
        String type = ceMetadata.getType();
        if (type == null) {
            type = configuration.getCloudEventsType().orElseThrow(
                    () -> new IllegalArgumentException("Cannot build the Cloud Event Record - type is not set"));
        }
        return type;
    }

    private static Optional<String> getSubject(OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {
        if (ceMetadata.getSubject().isPresent()) {
            return ceMetadata.getSubject();
        }
        return configuration.getCloudEventsSubject();
    }

    private static Optional<URI> getDataSchema(OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {
        if (ceMetadata.getDataSchema().isPresent()) {
            return ceMetadata.getDataSchema();
        }
        return configuration.getCloudEventsDataSchema().map(URI::create);
    }

    private static Optional<String> getDataContentType(OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {
        if (ceMetadata.getDataContentType().isPresent()) {
            return ceMetadata.getDataContentType();
        }
        return configuration.getCloudEventsDataContentType();
    }

    public static AmqpMessage createStructuredEventMessage(AmqpMessage message,
            OutgoingCloudEventMetadata<?> ceMetadata,
            AmqpConnectorOutgoingConfiguration configuration) {

        if (ceMetadata == null) {
            ceMetadata = OutgoingCloudEventMetadata.builder().build();
        }

        AmqpMessageBuilder builder = AmqpMessage.create(message);

        String source = getSource(ceMetadata, configuration);
        String type = getType(ceMetadata, configuration);
        Optional<String> subject = getSubject(ceMetadata, configuration);
        Optional<String> dataContentType = getDataContentType(ceMetadata, configuration);
        Optional<URI> schema = getDataSchema(ceMetadata, configuration);

        // We need to build the JSON Object representing the Cloud Event
        JsonObject json = new JsonObject();
        json.put(CE_ATTRIBUTE_SPEC_VERSION, ceMetadata.getSpecVersion())
                .put(CE_ATTRIBUTE_TYPE, type)
                .put(CE_ATTRIBUTE_SOURCE, source)
                .put(CE_ATTRIBUTE_ID, ceMetadata.getId());

        ZonedDateTime time = ceMetadata.getTimeStamp().orElse(null);
        if (time != null) {
            json.put(CE_ATTRIBUTE_TIME, time.toInstant());
        } else if (configuration.getCloudEventsInsertTimestamp()) {
            json.put(CE_ATTRIBUTE_TIME, Instant.now());
        }

        schema.ifPresent(s -> json.put(CE_ATTRIBUTE_DATA_SCHEMA, s));
        dataContentType.ifPresent(s -> json.put(CE_ATTRIBUTE_DATA_CONTENT_TYPE, s));
        subject.ifPresent(s -> json.put(CE_ATTRIBUTE_SUBJECT, s));

        // Extensions
        ceMetadata.getExtensions().forEach(json::put);

        // Encode the payload to json
        if (message.getDelegate().unwrap().getBody().getType() == Section.SectionType.AmqpValue) {
            json.put("data", message.bodyAsString());
        } else if (message.getDelegate().unwrap().getBody().getType() == Section.SectionType.Data) {
            json.put("data", message.bodyAsJsonObject());
        } else {
            throw new UnsupportedOperationException(
                    "Invalid payload for structure cloud events: " + message.getDelegate().unwrap().getBody());
        }

        builder.withJsonObjectAsBody(json);

        // Rule 3.2.1 - is content-type is not set, set it.
        // Must happen after having set the payload, as it sets the content type
        if (message.contentType() == null || !message.contentType().startsWith(CE_CONTENT_TYPE_PREFIX)) {
            builder.contentType(STRUCTURED_CONTENT_TYPE);
        }
        return builder.build();
    }

    public enum CloudEventMode {
        STRUCTURED,
        BINARY,
        NOT_A_CLOUD_EVENT
    }

    public static CloudEventMode getCloudEventMode(io.vertx.amqp.AmqpMessage incoming) {
        String contentType = incoming.contentType();
        if (contentType != null && contentType.startsWith(CE_CONTENT_TYPE_PREFIX)) {
            return CloudEventMode.STRUCTURED;
        } else if (containsAllMandatoryAttributes(incoming)) {
            return CloudEventMode.BINARY;
        }
        return CloudEventMode.NOT_A_CLOUD_EVENT;
    }

    private static boolean containsAllMandatoryAttributes(io.vertx.amqp.AmqpMessage incoming) {
        JsonObject app = incoming.applicationProperties();
        if (app == null || app.isEmpty()) {
            return false;
        }
        return app.getString(AMQP_HEADER_FOR_ID) != null
                && app.getString(AMQP_HEADER_FOR_SOURCE) != null
                && app.getString(AMQP_HEADER_FOR_TYPE) != null
                && app.getString(AMQP_HEADER_FOR_SPEC_VERSION) != null;
    }
}
