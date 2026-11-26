package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.converters.KafkaMessageTypeConverter;

/**
 * Graceful shutdown is enabled by default for Kafka channels, and its in-flight tracking rebuilds messages, which lose
 * the Kafka message type. Methods still consuming the (deprecated) Kafka message types get them restored.
 */
public class KafkaMessageTypeRestoreTest extends KafkaCompanionTestBase {

    @Test
    void recordConsumerReceivesKafkaRecords() {
        String group = "test-restore-record";
        addBeans(KafkaMessageTypeConverter.class, RecordConsumer.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "k-" + i, i), 10);

        RecordConsumer bean = runApplication(config(group), RecordConsumer.class);
        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.received().size() == 10);

        List<KafkaRecord<String, Integer>> received = bean.received();
        assertThat(received).allSatisfy(record -> assertThat(record).isInstanceOf(IncomingKafkaRecord.class));
        assertThat(received).extracting(KafkaRecord::getKey).contains("k-0", "k-9");

        container.close();
        container = null;
        assertCommitted(group, 10);
    }

    @Test
    void batchConsumerReceivesKafkaRecordBatches() {
        String group = "test-restore-batch";
        addBeans(KafkaMessageTypeConverter.class, BatchConsumer.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topic, "k-" + i, i), 10);

        KafkaMapBasedConfig config = config(group);
        config.put("batch", true);
        BatchConsumer bean = runApplication(config, BatchConsumer.class);
        await().atMost(30, TimeUnit.SECONDS).until(() -> bean.recordCount() == 10);

        assertThat(bean.batches()).allSatisfy(batch -> assertThat(batch).isInstanceOf(IncomingKafkaRecordBatch.class));

        container.close();
        container = null;
        assertCommitted(group, 10);
    }

    private KafkaMapBasedConfig config(String group) {
        // No graceful-shutdown property: Kafka enables it by default, so messages are tracked (and rebuilt).
        // Remove the test-harness default (KafkaMapBasedConfig forces graceful-shutdown=false for incoming channels).
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.data");
        config.remove("mp.messaging.incoming.data.graceful-shutdown");
        config.put("group.id", group);
        config.put("topic", topic);
        config.put("value.deserializer", IntegerDeserializer.class.getName());
        config.put("enable.auto.commit", "false");
        config.put("auto.offset.reset", "earliest");
        return config;
    }

    private void assertCommitted(String group, long expected) {
        TopicPartition tp = new TopicPartition(topic, 0);
        await().untilAsserted(() -> {
            OffsetAndMetadata offset = companion.consumerGroups().offsets(group, tp);
            assertThat(offset).isNotNull();
            assertThat(offset.offset()).isEqualTo(expected);
        });
    }

    @ApplicationScoped
    public static class RecordConsumer {

        private final List<KafkaRecord<String, Integer>> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(KafkaRecord<String, Integer> record) {
            received.add(record);
            return record.ack();
        }

        public List<KafkaRecord<String, Integer>> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class BatchConsumer {

        private final List<KafkaRecordBatch<String, Integer>> batches = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(KafkaRecordBatch<String, Integer> batch) {
            batches.add(batch);
            return batch.ack();
        }

        public List<KafkaRecordBatch<String, Integer>> batches() {
            return batches;
        }

        public int recordCount() {
            return batches.stream().mapToInt(batch -> batch.getRecords().size()).sum();
        }
    }
}
