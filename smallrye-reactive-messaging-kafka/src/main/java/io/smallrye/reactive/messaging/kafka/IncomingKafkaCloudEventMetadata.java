package io.smallrye.reactive.messaging.kafka;

import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;

/**
 * Addition "extension" attribute specific to incoming Kafka record
 *
 * @param <T> the data type
 */
public interface IncomingKafkaCloudEventMetadata<K, T> extends IncomingCloudEventMetadata<T> {

    /**
     * Extension - attribute name associated with the record's key.
     * Defined in the Kafka Protocol Binding for Cloud Events
     */
    String CE_KAFKA_KEY = "partitionkey";

    /**
     * Extension - attribute name associated with the record's topic.
     */
    String CE_KAFKA_TOPIC = "kafkatopic";

    /**
     * @return the record's key, @{code null} if none
     */
    @SuppressWarnings("unchecked")
    default K getKey() {
        return (K) getExtension(CE_KAFKA_KEY).orElse(null);
    }

    /**
     * @return the topic
     */
    default String getTopic() {
        return (String) getExtension(CE_KAFKA_TOPIC)
                .orElseThrow(() -> new IllegalStateException("Topic should have been added to the Cloud Event extension"));
    }

}
