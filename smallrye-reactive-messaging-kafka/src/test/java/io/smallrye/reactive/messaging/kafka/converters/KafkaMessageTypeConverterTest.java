package io.smallrye.reactive.messaging.kafka.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecord;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

class KafkaMessageTypeConverterTest {

    private final KafkaMessageTypeConverter converter = new KafkaMessageTypeConverter();

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();

    /**
     * A message as rebuilt by an interception: Kafka metadata and acknowledgement kept, Kafka type lost.
     */
    private <T> Message<T> intercepted(T payload, Object metadata) {
        return Message.of(payload, Metadata.of(metadata),
                () -> {
                    acks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                },
                failure -> {
                    nacks.incrementAndGet();
                    return CompletableFuture.completedFuture(null);
                });
    }

    private static IncomingKafkaRecordMetadata<String, Integer> recordMetadata(int partition, long offset, int value) {
        return new IncomingKafkaRecordMetadata<>(new ConsumerRecord<>("topic", partition, offset, "key-" + offset, value),
                "channel");
    }

    @Test
    void canConvertOnlyKafkaRecordTargetsOfInterceptedRecords() {
        Message<Integer> message = intercepted(1, recordMetadata(0, 10, 1));

        assertThat(converter.canConvert(message, KafkaRecord.class)).isTrue();
        assertThat(converter.canConvert(message, IncomingKafkaRecord.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, KafkaRecordBatch.class)).isFalse();
        assertThat(converter.canConvert(Message.of(1), KafkaRecord.class)).isFalse();

        // Already a Kafka record: nothing to restore.
        Message<?> restored = converter.convert(message, KafkaRecord.class);
        assertThat(converter.canConvert(restored, KafkaRecord.class)).isFalse();
    }

    @Test
    void restoresARecordAndDelegatesAcknowledgement() {
        Message<Integer> message = intercepted(7, recordMetadata(0, 42, 7));

        Message<?> converted = converter.convert(message, IncomingKafkaRecord.class);

        assertThat(converted).isInstanceOf(IncomingKafkaRecord.class);
        IncomingKafkaRecord<?, ?> record = (IncomingKafkaRecord<?, ?>) converted;
        assertThat(record.getPayload()).isEqualTo(7);
        assertThat(record.getKey()).isEqualTo("key-42");
        assertThat(record.getTopic()).isEqualTo("topic");
        assertThat(record.getPartition()).isZero();
        assertThat(record.getOffset()).isEqualTo(42);
        assertThat(record.getMetadata(IncomingKafkaRecordMetadata.class)).isPresent();

        record.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        record.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(1);
    }

    @Test
    void restoresABatchAndDelegatesAcknowledgement() {
        List<Message<?>> batched = List.of(
                intercepted(1, recordMetadata(0, 1, 1)),
                intercepted(2, recordMetadata(0, 2, 2)),
                intercepted(3, recordMetadata(1, 5, 3)));
        IncomingKafkaRecordBatchMetadata<String, Integer> batchMetadata = new IncomingKafkaRecordBatchMetadata<>(
                ConsumerRecords.empty(), batched, "channel", 0, Map.of(), -1);
        Message<List<Integer>> message = intercepted(List.of(1, 2, 3), batchMetadata);

        assertThat(converter.canConvert(message, KafkaRecordBatch.class)).isTrue();
        assertThat(converter.canConvert(message, KafkaRecord.class)).isFalse();

        Message<?> converted = converter.convert(message, KafkaRecordBatch.class);

        assertThat(converted).isInstanceOf(IncomingKafkaRecordBatch.class);
        @SuppressWarnings("unchecked")
        IncomingKafkaRecordBatch<String, Integer> batch = (IncomingKafkaRecordBatch<String, Integer>) converted;
        assertThat(batch.getPayload()).containsExactly(1, 2, 3);
        assertThat(batch.getRecords()).hasSize(3)
                .allSatisfy(record -> assertThat(record).isInstanceOf(IncomingKafkaRecord.class));
        assertThat(batch.getLatestOffsetRecords()).hasSize(2);
        assertThat(((IncomingKafkaRecord<?, ?>) batch.getLatestOffsetRecords().get(new TopicPartition("topic", 0)))
                .getOffset()).isEqualTo(2);
        assertThat(((IncomingKafkaRecord<?, ?>) batch.getLatestOffsetRecords().get(new TopicPartition("topic", 1)))
                .getOffset()).isEqualTo(5);

        // Acknowledging the batch delegates to the intercepted batch message, once.
        batch.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
    }
}
