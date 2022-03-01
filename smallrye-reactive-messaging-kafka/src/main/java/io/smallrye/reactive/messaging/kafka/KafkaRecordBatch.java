package io.smallrye.reactive.messaging.kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

/**
 * Represents a batch of Kafka records received by polling the {@link org.apache.kafka.clients.consumer.KafkaConsumer}
 *
 * This type extends the {@code Message<List<T>>} where {@code T} is the type of records' payloads.
 * The complete list of Kafka record payloads are accessible via the {@link Message#getPayload()} method.
 *
 * @param <K> The record key type
 * @param <T> The record payload type
 */
public interface KafkaRecordBatch<K, T> extends Message<List<T>>, Iterable<KafkaRecord<K, T>>, ContextAwareMessage<List<T>> {
    /**
     * @return list of records contained in this message batch
     */
    List<KafkaRecord<K, T>> getRecords();

    /**
     * @return map of records with latest offset by topic partition
     */
    Map<TopicPartition, KafkaRecord<K, T>> getLatestOffsetRecords();
}
