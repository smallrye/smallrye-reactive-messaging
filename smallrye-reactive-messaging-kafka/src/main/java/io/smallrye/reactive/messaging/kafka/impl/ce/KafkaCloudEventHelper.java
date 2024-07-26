package io.smallrye.reactive.messaging.kafka.impl.ce;

import static io.smallrye.reactive.messaging.ce.CloudEventMetadata.*;
import static io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata.CE_KAFKA_KEY;
import static io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata.CE_KAFKA_TOPIC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.DefaultCloudEventMetadataBuilder;
import io.smallrye.reactive.messaging.ce.OutgoingCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.impl.BaseCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.impl.DefaultIncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.impl.KafkaRecordHelper;
import io.smallrye.reactive.messaging.kafka.impl.RuntimeKafkaSinkConfiguration;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;

public class KafkaCloudEventHelper {

    public static final String KAFKA_HEADER_CONTENT_TYPE = "content-type";
    public static final String CE_CONTENT_TYPE_PREFIX = "application/cloudevents";
    public static final String CE_HEADER_PREFIX = "ce_";
    public static final String STRUCTURED_CONTENT_TYPE = CE_CONTENT_TYPE_PREFIX + "+json; charset=UTF-8";

    public static final String KAFKA_HEADER_FOR_SPEC_VERSION = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SPEC_VERSION;
    public static final String KAFKA_HEADER_FOR_TYPE = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_TYPE;
    public static final String KAFKA_HEADER_FOR_SOURCE = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SOURCE;
    public static final String KAFKA_HEADER_FOR_ID = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_ID;

    public static final String KAFKA_HEADER_FOR_SCHEMA = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_DATA_SCHEMA;
    public static final String KAFKA_HEADER_FOR_CONTENT_TYPE = CE_HEADER_PREFIX
            + CloudEventMetadata.CE_ATTRIBUTE_DATA_CONTENT_TYPE;
    public static final String KAFKA_HEADER_FOR_SUBJECT = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SUBJECT;
    public static final String KAFKA_HEADER_FOR_TIME = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_TIME;

    // TODO Should be replaced with DateTimeFormatter.ISO_OFFSET_DATE_TIME, there for read retro-compatibility
    public static final DateTimeFormatter RFC3339_DATE_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .appendZoneOrOffsetId()
            .toFormatter();

    private KafkaCloudEventHelper() {
        // avoid direct instantiation
    }

    public static <T, K> IncomingKafkaCloudEventMetadata<K, T> createFromStructuredCloudEvent(
            ConsumerRecord<K, T> record) {
        DefaultCloudEventMetadataBuilder<T> builder = new DefaultCloudEventMetadataBuilder<>();

        JsonObject content = (JsonObject) record.value();

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

        // Extensions
        if (record.key() != null) {
            builder.withExtension(CE_KAFKA_KEY, record.key());
        }
        builder.withExtension(CE_KAFKA_TOPIC, record.topic());

        // Data
        Object data = content.getValue("data");
        //noinspection unchecked
        builder
                .withData((T) data);

        BaseCloudEventMetadata<T> cloudEventMetadata = builder.build();
        cloudEventMetadata.validate();
        return new DefaultIncomingKafkaCloudEventMetadata<>(
                new DefaultIncomingCloudEventMetadata<>(cloudEventMetadata));
    }

    public static JsonObject parseStructuredContent(Object value) {
        JsonObject content;
        if (value instanceof JsonObject) {
            content = (JsonObject) value;
        } else if (value instanceof String) {
            content = new JsonObject((String) value);
        } else if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            Buffer buffer = Buffer.buffer(bytes);
            content = buffer.toJsonObject();
        } else {
            throw new IllegalArgumentException(
                    "Invalid value type. Structured Cloud Event can only be created from String, JsonObject and byte[], found: "
                            + Optional.ofNullable(value).map(Object::getClass).orElse(null));
        }
        // validate required source attribute
        String source = content.getString(CloudEventMetadata.CE_ATTRIBUTE_SOURCE);
        if (source == null) {
            throw new IllegalArgumentException(
                    "The JSON value must contain the " + CloudEventMetadata.CE_ATTRIBUTE_SOURCE + " attribute");
        }
        return content;
    }

    public static <T, K> IncomingKafkaCloudEventMetadata<K, T> createFromBinaryCloudEvent(
            ConsumerRecord<?, T> record) {
        DefaultCloudEventMetadataBuilder<T> builder = new DefaultCloudEventMetadataBuilder<>();

        // Build a map containing all the headers, expect null values
        // We remove the entry at each access to filter out extension attribute.
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(kh -> {
            // null values from arbitrary headers could break the UTF-8 conversion
            if (kh.value() != null) {
                String key = kh.key();
                String value = new String(kh.value(), StandardCharsets.UTF_8); // Rules 3.2.3 - Force UTF-8
                headers.put(key, value);
            }
        });

        // Required
        builder.withSpecVersion(headers.remove(KAFKA_HEADER_FOR_SPEC_VERSION));
        builder.withId(headers.remove(KAFKA_HEADER_FOR_ID));
        String source = headers.remove(KAFKA_HEADER_FOR_SOURCE);
        if (source == null) {
            throw new IllegalArgumentException(
                    "The Kafka record must contain the " + KAFKA_HEADER_FOR_SOURCE + " header");
        }
        builder.withSource(URI.create(source));
        builder.withType(headers.remove(KAFKA_HEADER_FOR_TYPE));

        // Optional

        // Rules 3.2.1 - Set datacontenttype to the record's content type header
        String ct = headers.remove(KAFKA_HEADER_CONTENT_TYPE);
        if (ct != null) {
            builder.withDataContentType(ct);
        }

        String schema = headers.remove(KAFKA_HEADER_FOR_SCHEMA);
        if (schema != null) {
            builder.withDataSchema(URI.create(schema));
        }

        String subject = headers.remove(KAFKA_HEADER_FOR_SUBJECT);
        if (subject != null) {
            builder.withSubject(subject);
        }

        String time = headers.remove(KAFKA_HEADER_FOR_TIME);
        if (time != null) {
            ZonedDateTime parse = ZonedDateTime.parse(time, RFC3339_DATE_FORMAT);
            builder.withTimestamp(parse);
        }

        // Extensions
        if (record.key() != null) {
            builder.withExtension(CE_KAFKA_KEY, record.key());
        }
        builder.withExtension(CE_KAFKA_TOPIC, record.topic());

        headers.entrySet().stream().filter(entry -> entry.getKey().startsWith(CE_HEADER_PREFIX)).forEach(entry -> {
            String key = entry.getKey().substring(CE_HEADER_PREFIX.length());
            // Implementation choice: Extension attributes are stored as String.
            builder.withExtension(key, entry.getValue());
        });

        // Data
        builder
                .withData(record.value());

        BaseCloudEventMetadata<T> cloudEventMetadata = builder.build();
        return new DefaultIncomingKafkaCloudEventMetadata<>(
                new DefaultIncomingCloudEventMetadata<>(cloudEventMetadata));
    }

    public static <T> T checkBinaryRecord(T payload, Headers headers) {
        Header sourceHeader = headers.lastHeader(KAFKA_HEADER_FOR_SOURCE);
        if (sourceHeader == null) {
            throw new IllegalArgumentException(
                    "The Kafka record must contain the " + KAFKA_HEADER_FOR_SOURCE + " header");
        }
        return payload;
    }

    @SuppressWarnings("rawtypes")
    public static ProducerRecord<?, ?> createBinaryRecord(Message<?> message, String topic,
            OutgoingKafkaRecordMetadata<?> outgoingMetadata, IncomingKafkaRecordMetadata<?, ?> incomingMetadata,
            OutgoingCloudEventMetadata<?> ceMetadata, RuntimeKafkaSinkConfiguration configuration) {

        if (ceMetadata == null) {
            ceMetadata = OutgoingCloudEventMetadata.builder().build();
        }
        Integer partition = getPartition(outgoingMetadata, configuration);
        Object key = getKey(message, outgoingMetadata, ceMetadata, configuration);
        Long timestamp = getTimestamp(outgoingMetadata);
        Headers headers = KafkaRecordHelper.getHeaders(outgoingMetadata, incomingMetadata, configuration);

        Optional<String> subject = getSubject(ceMetadata, configuration);
        Optional<String> contentType = getDataContentType(ceMetadata, configuration);
        Optional<URI> schema = getDataSchema(ceMetadata, configuration);

        // Add the Cloud Event header - prefixed with ce_ (rules 3.2.3.1)
        // Mandatory headers
        headers.add(new RecordHeader(KAFKA_HEADER_FOR_SPEC_VERSION,
                ceMetadata.getSpecVersion().getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(KAFKA_HEADER_FOR_ID, ceMetadata.getId().getBytes(StandardCharsets.UTF_8)));

        String type = getType(ceMetadata, configuration);
        headers.add(new RecordHeader(KAFKA_HEADER_FOR_TYPE, type.getBytes(StandardCharsets.UTF_8)));

        String source = getSource(ceMetadata, configuration);
        headers.add(new RecordHeader(KAFKA_HEADER_FOR_SOURCE, source.getBytes(StandardCharsets.UTF_8)));

        // Optional attribute
        subject.ifPresent(
                s -> headers.add(new RecordHeader(KAFKA_HEADER_FOR_SUBJECT, s.getBytes(StandardCharsets.UTF_8))));
        contentType.ifPresent(
                s -> {
                    headers.add(new RecordHeader(KAFKA_HEADER_FOR_CONTENT_TYPE, s.getBytes(StandardCharsets.UTF_8)));
                    // Rules 3.2.1 - in binary mode, the content-type header must be mapped to the datacontenttype attribute.
                    headers.add(new RecordHeader("content-type", s.getBytes(StandardCharsets.UTF_8)));
                });
        schema.ifPresent(
                s -> headers.add(new RecordHeader(KAFKA_HEADER_FOR_SCHEMA, s.toString().getBytes(StandardCharsets.UTF_8))));

        Optional<ZonedDateTime> ts = ceMetadata.getTimeStamp();
        if (ts.isPresent()) {
            ZonedDateTime time = ts.get();
            headers.add(new RecordHeader(KAFKA_HEADER_FOR_TIME,
                    ISO_OFFSET_DATE_TIME.format(time).getBytes(StandardCharsets.UTF_8)));
        } else if (timestamp != null) {
            Instant instant = Instant.ofEpochMilli(timestamp);
            headers.add(new RecordHeader(KAFKA_HEADER_FOR_TIME,
                    ISO_OFFSET_DATE_TIME.format(instant).getBytes(StandardCharsets.UTF_8)));
        } else if (configuration.getCloudEventsInsertTimestamp()) {
            ZonedDateTime now = ZonedDateTime.now();
            headers.add(new RecordHeader(KAFKA_HEADER_FOR_TIME,
                    ISO_OFFSET_DATE_TIME.format(now).getBytes(StandardCharsets.UTF_8)));
        }

        // Extensions
        ceMetadata.getExtensions().forEach((k, v) -> {
            if (v != null) {
                headers.add(new RecordHeader(CE_HEADER_PREFIX + k, v.toString().getBytes(StandardCharsets.UTF_8)));
            }
        });

        Object payload = message.getPayload();
        if (payload instanceof Record) {
            payload = ((Record) payload).value();
        }
        return new ProducerRecord<>(
                topic,
                partition,
                timestamp,
                key,
                payload,
                headers);
    }

    private static String getSource(OutgoingCloudEventMetadata<?> ceMetadata,
            RuntimeKafkaSinkConfiguration configuration) {
        String source = ceMetadata.getSource() != null ? ceMetadata.getSource().toString() : null;
        if (source == null) {
            source = configuration.getCloudEventsSource().orElseThrow(() -> new IllegalArgumentException(
                    "Cannot build the Cloud Event Record - source is not set"));
        }
        return source;
    }

    private static String getType(OutgoingCloudEventMetadata<?> ceMetadata,
            RuntimeKafkaSinkConfiguration configuration) {
        String type = ceMetadata.getType();
        if (type == null) {
            type = configuration.getCloudEventsType().orElseThrow(
                    () -> new IllegalArgumentException("Cannot build the Cloud Event Record - type is not set"));
        }
        return type;
    }

    private static Optional<String> getSubject(OutgoingCloudEventMetadata<?> ceMetadata,
            RuntimeKafkaSinkConfiguration configuration) {
        if (ceMetadata.getSubject().isPresent()) {
            return ceMetadata.getSubject();
        }
        return configuration.getCloudEventsSubject();
    }

    private static Optional<URI> getDataSchema(OutgoingCloudEventMetadata<?> ceMetadata,
            RuntimeKafkaSinkConfiguration configuration) {
        if (ceMetadata.getDataSchema().isPresent()) {
            return ceMetadata.getDataSchema();
        }
        return configuration.getCloudEventsDataSchema().map(URI::create);
    }

    private static Optional<String> getDataContentType(OutgoingCloudEventMetadata<?> ceMetadata,
            RuntimeKafkaSinkConfiguration configuration) {
        if (ceMetadata.getDataContentType().isPresent()) {
            return ceMetadata.getDataContentType();
        }
        return configuration.getCloudEventsDataContentType();
    }

    private static Long getTimestamp(OutgoingKafkaRecordMetadata<?> metadata) {
        long timestamp = -1;
        if (metadata != null && metadata.getTimestamp() != null) {
            timestamp = metadata.getTimestamp().toEpochMilli();
        }
        if (timestamp <= 0) {
            return null;
        }
        return timestamp;
    }

    @SuppressWarnings({ "rawtypes" })
    private static Object getKey(Message<?> message,
            OutgoingKafkaRecordMetadata<?> metadata, OutgoingCloudEventMetadata<?> ceMetadata,
            RuntimeKafkaSinkConfiguration configuration) {

        // First, the message metadata
        if (metadata != null && metadata.getKey() != null) {
            return metadata.getKey();
        }

        // Then, check if the message payload is a record
        if (message.getPayload() instanceof Record) {
            return ((Record) message.getPayload()).key();
        }

        // Finally, check the Cloud Event metadata, if not there, the Kafka connector config
        return ceMetadata.getExtension(CE_KAFKA_KEY)
                .orElse(configuration.getKey().orElse(null));
    }

    private static Integer getPartition(OutgoingKafkaRecordMetadata<?> metadata,
            RuntimeKafkaSinkConfiguration configuration) {
        int partition = configuration.getPartition();
        if (metadata != null && metadata.getPartition() != -1) {
            partition = metadata.getPartition();
        }
        if (partition < 0) {
            return null;
        }
        return partition;
    }

    @SuppressWarnings("rawtypes")
    public static ProducerRecord<?, ?> createStructuredRecord(Message<?> message, String topic,
            OutgoingKafkaRecordMetadata<?> outgoingMetadata, IncomingKafkaRecordMetadata<?, ?> incomingMetadata,
            OutgoingCloudEventMetadata<?> ceMetadata, RuntimeKafkaSinkConfiguration configuration) {

        if (ceMetadata == null) {
            ceMetadata = OutgoingCloudEventMetadata.builder().build();
        }

        Integer partition = getPartition(outgoingMetadata, configuration);
        Object key = getKey(message, outgoingMetadata, ceMetadata, configuration);
        Long timestamp = getTimestamp(outgoingMetadata);
        Headers headers = KafkaRecordHelper.getHeaders(outgoingMetadata, incomingMetadata, configuration);

        String source = getSource(ceMetadata, configuration);
        String type = getType(ceMetadata, configuration);
        Optional<String> subject = getSubject(ceMetadata, configuration);
        Optional<String> dataContentType = getDataContentType(ceMetadata, configuration);
        Optional<URI> schema = getDataSchema(ceMetadata, configuration);

        // if headers does not contain a "content-type" header add one
        Optional<Header> contentType = StreamSupport.stream(headers.spliterator(), false)
                .filter(h -> h.key().equalsIgnoreCase(KAFKA_HEADER_CONTENT_TYPE))
                .findFirst();
        if (!contentType.isPresent()) {
            headers.add(new RecordHeader(KAFKA_HEADER_CONTENT_TYPE, STRUCTURED_CONTENT_TYPE.getBytes()));
        }

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
        Object payload = message.getPayload();
        if (payload instanceof Record) {
            payload = ((Record) payload).value();
        }
        if (payload instanceof String) {
            json.put("data", payload);
        } else {
            json.put("data", JsonObject.mapFrom(payload));
        }

        return new ProducerRecord<>(topic, partition, timestamp, key, json.encode(), headers);
    }

    public enum CloudEventMode {
        STRUCTURED,
        BINARY,
        NOT_A_CLOUD_EVENT
    }

    public static CloudEventMode getCloudEventMode(Headers headers) {
        String contentType = getHeader(KAFKA_HEADER_CONTENT_TYPE, headers);
        if (contentType != null && contentType.startsWith(CE_CONTENT_TYPE_PREFIX)) {
            return CloudEventMode.STRUCTURED;
        } else if (containsAllMandatoryAttributes(headers)) {
            return CloudEventMode.BINARY;
        }
        return CloudEventMode.NOT_A_CLOUD_EVENT;
    }

    private static boolean containsAllMandatoryAttributes(Headers headers) {
        return getHeader(KAFKA_HEADER_FOR_ID, headers) != null
                && getHeader(KAFKA_HEADER_FOR_SOURCE, headers) != null
                && getHeader(KAFKA_HEADER_FOR_TYPE, headers) != null
                && getHeader(KAFKA_HEADER_FOR_SPEC_VERSION, headers) != null;
    }

    private static String getHeader(String name, Headers headers) {
        for (Header header : headers) {
            if (header.key().equals(name)) {
                return new String(header.value(), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

}
