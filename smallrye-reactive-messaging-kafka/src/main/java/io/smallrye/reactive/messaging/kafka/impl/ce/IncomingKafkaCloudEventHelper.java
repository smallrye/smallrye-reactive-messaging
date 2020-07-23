package io.smallrye.reactive.messaging.kafka.impl.ce;

import static io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata.CE_KAFKA_KEY;
import static io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata.CE_KAFKA_TOPIC;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.smallrye.reactive.messaging.ce.CloudEventMetadata;
import io.smallrye.reactive.messaging.ce.DefaultCloudEventMetadata;
import io.smallrye.reactive.messaging.ce.DefaultCloudEventMetadataBuilder;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaCloudEventMetadata;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.mutiny.kafka.client.producer.KafkaHeader;

public class IncomingKafkaCloudEventHelper {

    public static final String KAFKA_HEADER_CONTENT_TYPE = "content-type";
    public static final String CE_CONTENT_TYPE_PREFIX = "application/cloudevents";
    public static final String CE_HEADER_PREFIX = "ce_";

    public static final String KAFKA_HEADER_FOR_SPEC_VERSION = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SPEC_VERSION;
    public static final String KAFKA_HEADER_FOR_TYPE = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_TYPE;
    public static final String KAFKA_HEADER_FOR_SOURCE = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SOURCE;
    public static final String KAFKA_HEADER_FOR_ID = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_ID;

    public static final String KAFKA_HEADER_FOR_SCHEMA = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_DATA_SCHEMA;
    public static final String KAFKA_HEADER_FOR_SUBJECT = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_SUBJECT;
    public static final String KAFKA_HEADER_FOR_TIME = CE_HEADER_PREFIX + CloudEventMetadata.CE_ATTRIBUTE_TIME;

    private IncomingKafkaCloudEventHelper() {
        // avoid direct instantiation
    }

    public static <T, K> IncomingKafkaCloudEventMetadata<K, T> createFromStructuredCloudEvent(
            KafkaConsumerRecord<K, T> record) {
        DefaultCloudEventMetadataBuilder<T> builder = CloudEventMetadata.builder();

        JsonObject content;
        if (record.value() instanceof JsonObject) {
            content = (JsonObject) record.value();
        } else if (record.value() instanceof String) {
            content = new JsonObject((String) record.value());
        } else if (record.value() instanceof byte[]) {
            byte[] bytes = (byte[]) record.value();
            Buffer buffer = Buffer.buffer(bytes);
            content = buffer.toJsonObject();
        } else {
            throw new IllegalArgumentException(
                    "Invalid value type. Structured Cloud Event can only be created from String, JsonObject and byte[], found: "
                            + record.value().getClass());
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
            builder.withTimestamp(ZonedDateTime.parse(time));
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

        DefaultCloudEventMetadata<T> cloudEventMetadata = builder.build();
        return new DefaultIncomingKafkaCloudEventMetadata<>(cloudEventMetadata);
    }

    public static <T, K> IncomingKafkaCloudEventMetadata<K, T> createFromBinaryCloudEvent(
            KafkaConsumerRecord<?, T> record) {
        DefaultCloudEventMetadataBuilder<T> builder = CloudEventMetadata.builder();

        // Build a map containing all the headers
        // We remove the entry at each access to filter out extension attribute.
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(kh -> {
            String key = kh.key();
            String value = kh.value().toString("UTF-8"); // Rules 3.2.3 - Force UTF-8
            headers.put(key, value);
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
            builder.withTimestamp(ZonedDateTime.parse(time));
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

        DefaultCloudEventMetadata<T> cloudEventMetadata = builder.build();
        return new DefaultIncomingKafkaCloudEventMetadata<>(cloudEventMetadata);
    }

    public enum CloudEventMode {
        STRUCTURED,
        BINARY,
        NOT_A_CLOUD_EVENT
    }

    public static CloudEventMode getCloudEventMode(KafkaConsumerRecord<?, ?> record) {
        String contentType = getHeader(KAFKA_HEADER_CONTENT_TYPE, record);
        if (contentType != null && contentType.startsWith(CE_CONTENT_TYPE_PREFIX)) {
            return CloudEventMode.STRUCTURED;
        } else if (containsAllMandatoryAttributes(record)) {
            return CloudEventMode.BINARY;
        }
        return CloudEventMode.NOT_A_CLOUD_EVENT;
    }

    private static boolean containsAllMandatoryAttributes(KafkaConsumerRecord<?, ?> record) {
        return getHeader(KAFKA_HEADER_FOR_ID, record) != null
                && getHeader(KAFKA_HEADER_FOR_SOURCE, record) != null
                && getHeader(KAFKA_HEADER_FOR_TYPE, record) != null
                && getHeader(KAFKA_HEADER_FOR_SPEC_VERSION, record) != null;
    }

    private static String getHeader(String name, KafkaConsumerRecord<?, ?> record) {
        List<KafkaHeader> headers = record.headers();
        for (KafkaHeader header : headers) {
            if (header.key().equals(name)) {
                return header.value().toString("UTF-8");
            }
        }
        return null;

    }

}
