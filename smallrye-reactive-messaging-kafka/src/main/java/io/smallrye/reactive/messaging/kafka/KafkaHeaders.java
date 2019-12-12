package io.smallrye.reactive.messaging.kafka;

import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * The list of Message headers understood by the connector.
 * These headers can be used on outgoing message to influence the dispatching (specific topic...), or used to
 * retrieve Kafka metadata.
 */
public class KafkaHeaders {

    /**
     * The topic of the record.
     * Type: String
     * {@code null} if not set.
     */
    public static final String TOPIC = "kafka.topic";

    /**
     * Allow attaching the outgoing topic to a message.
     * This header is used to indicate the outgoing message should be sent to this Kafka topic.
     * Type: String
     */
    public static final String OUTGOING_TOPIC = "kafka.outgoing-topic";

    /**
     * The record partition.
     * Type: int
     * {@code -1} if not set.
     */
    public static final String PARTITION = "kafka.partition";

    /**
     * Allow attaching the outgoing partition to a message.
     * This header is used to indicate the partition for the outgoing message. When the message get written into the
     * Kafka topic, this partition will be used.
     * Type: int
     */
    public static final String OUTGOING_PARTITION = "kafka.outgoing-partition";

    /**
     * The record headers.
     * Type: {@code Iterable<RecordHeader>}
     * {@code null} if not set.
     */
    public static final String HEADERS = "kafka.headers";

    /**
     * Allow attaching the headers to an outgoing message.
     * This header is used to indicate headers to be attached with an outgoing Kafka message.
     * Type: {@code Iterable<RecordHeader>}.
     */
    public static final String OUTGOING_HEADERS = "kafka.outgoing-headers";

    /**
     * The record key.
     * {@code null} if not set.
     */
    public static final String KEY = "kafka.key";

    /**
     * Allow attaching a key to an outgoing message.
     * This header is used to indicate the key to be used for the outgoing Kafka message.
     * Type: Any, depends on the key serializer, {@code String} if not key serializer is configured.
     */
    public static final String OUTGOING_KEY = "kafka.outgoing-key";

    /**
     * The record timestamp.
     * {@code -1} if not set.
     */
    public static final String TIMESTAMP = "kafka.timestamp";

    /**
     * Allow attaching a timestamp to an outgoing message.
     * This header is used to indicate the timestamp to be used for the outgoing Kafka message.
     * Type: long.
     */
    public static final String OUTGOING_TIMESTAMP = "kafka.outgoing-timestamp";

    /**
     * The record timestamp type.
     * {@code null} if not set.
     */
    public static final String TIMESTAMP_TYPE = "kafka.timestamp-type";

    /**
     * The record offset in the topic/partition.
     * {@code -1} if not set.
     */
    public static final String OFFSET = "kafka.offset";

    private KafkaHeaders() {
        // avoid direct instantiation
    }

    static String getKafkaTopic(Message<?> message, String defaultValue) {
        return message.getHeaders().getAsString(OUTGOING_TOPIC, defaultValue);
    }

    static int getKafkaPartition(Message<?> message, int defaultValue) {
        return message.getHeaders().getAsInteger(OUTGOING_PARTITION, defaultValue);
    }

    static String getKafkaKey(Message<?> message, String defaultValue) {
        return message.getHeaders().getAsString(OUTGOING_KEY, defaultValue);
    }

    static long getKafkaTimestamp(Message<?> message, long defaultValue) {
        return message.getHeaders().getAsLong(OUTGOING_TIMESTAMP, defaultValue);
    }

    @SuppressWarnings("unchecked")
    static Iterable<Header> getKafkaRecordHeaders(Message<?> message) {
        return message.getHeaders().get(OUTGOING_HEADERS, Iterable.class);
    }
}
