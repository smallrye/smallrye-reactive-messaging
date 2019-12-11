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
     * Must be a {@code String}.
     */
    public static final String TOPIC = "kafka.topic";

    /**
     * The record partition.
     * {@code -1} if not set.
     */
    public static final String PARTITION = "kafka.partition";

    /**
     * The record headers.
     * {@code null} if not set.
     */
    public static final String KAFKA_HEADERS = "kafka.headers";

    /**
     * The record key.
     * {@code null} if not set.
     */
    public static final String KEY = "kafka.key";

    /**
     * The record timestamp.
     * {@code -1} if not set.
     */
    public static final String TIMESTAMP = "kafka.timestamp";

    /**
     * The record timestamp type.
     * {@code null} if not set.
     */
    public static final String KAFKA_TIMESTAMP_TYPE = "kafka.timestamp-type";

    /**
     * The record offset in the topic/partition.
     * {@code -1} if not set.
     */
    public static final String OFFSET = "kafka.offset";

    private KafkaHeaders() {
        // avoid direct instantiation
    }

    public static String getKafkaTopic(Message<?> message, String defaultValue) {
        return message.getHeaders().getAsString(TOPIC, defaultValue);
    }

    public static int getKafkaPartition(Message<?> message, int defaultValue) {
        return message.getHeaders().getAsInteger(PARTITION, defaultValue);
    }

    public static String getKafkaKey(Message<?> message, String defaultValue) {
        return message.getHeaders().getAsString(KEY, defaultValue);
    }

    public static long getKafkaTimestamp(Message<?> message, long defaultValue) {
        return message.getHeaders().getAsLong(TIMESTAMP, defaultValue);
    }

    @SuppressWarnings("unchecked")
    public static Iterable<Header> getKafkaRecordHeaders(Message<?> message) {
        return message.getHeaders().get(KAFKA_HEADERS, Iterable.class);
    }
}
