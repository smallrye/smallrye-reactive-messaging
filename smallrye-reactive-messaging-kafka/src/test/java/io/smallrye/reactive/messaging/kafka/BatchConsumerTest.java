package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordBatchMetadata;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;

public class BatchConsumerTest extends KafkaCompanionTestBase {

    @Test
    void testIncomingConsumingListPayload() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic)
                .put("batch", true);

        BeanConsumingListPayload bean = runApplication(config, BeanConsumingListPayload.class);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, null, "v-" + i), 10);

        await().until(() -> bean.messages().size() == 10);

        assertThat(bean.messages()).hasSize(10).allSatisfy(p -> assertThat(p).startsWith("v-"));
    }

    @Test
    void testIncomingConsumingKafkaBatchRecords() {
        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic)
                .put("batch", true);
        BeanConsumingKafkaRecordBatch bean = runApplication(config, BeanConsumingKafkaRecordBatch.class);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "k-" + i, "v-" + i), 10);

        await().until(() -> bean.messages().size() == 10);

        assertThat(bean.messages()).hasSize(10).allSatisfy(r -> {
            assertThat(r.getKey()).startsWith("k-");
            assertThat(r.getPayload()).startsWith("v-");
        });
    }

    @Test
    void testIncomingConsumingMessageWithMetadata() {
        String newTopic = UUID.randomUUID().toString();
        companion.topics().createAndWait(newTopic, 3);

        KafkaMapBasedConfig config = kafkaConfig("mp.messaging.incoming.kafka")
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", newTopic)
                .put("batch", true);
        BeanConsumingMessageWithBatchMetadata bean = runApplication(config,
                BeanConsumingMessageWithBatchMetadata.class);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(newTopic, "k-" + i, "v-" + i), 10);

        await().until(() -> bean.metadata().stream().mapToInt(m -> m.getRecords().count()).sum() == 10);

        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        for (IncomingKafkaRecordBatchMetadata<String, String> metadata : bean.metadata()) {
            for (TopicPartition partition : metadata.getRecords().partitions()) {
                List<ConsumerRecord<String, String>> list = records.computeIfAbsent(partition, p -> new ArrayList<>());
                list.addAll(metadata.getRecords().records(partition));
            }
        }

        assertThat(records.keySet()).hasSize(3);
        assertThat(records.values()).flatMap(l -> l).hasSize(10).allSatisfy(r -> {
            assertThat(r.value()).startsWith("v-");
            assertThat(r.key()).startsWith("k");
        });
    }

    @ApplicationScoped
    public static class BeanConsumingListPayload {

        final List<String> messages = new CopyOnWriteArrayList<>();

        @Incoming("kafka")
        public void consume(List<String> records) {
            messages.addAll(records);
        }

        public List<String> messages() {
            return this.messages;
        }

    }

    @ApplicationScoped
    public static class BeanConsumingKafkaRecordBatch {

        final List<KafkaRecord<String, String>> messages = new CopyOnWriteArrayList<>();

        @Incoming("kafka")
        public CompletionStage<Void> consume(KafkaRecordBatch<String, String> records) {
            messages.addAll(records.getRecords());
            return records.ack();
        }

        public List<KafkaRecord<String, String>> messages() {
            return this.messages;
        }

    }

    @ApplicationScoped
    public static class BeanConsumingMessageWithBatchMetadata {

        final List<IncomingKafkaRecordBatchMetadata<String, String>> metadata = new CopyOnWriteArrayList<>();

        @Incoming("kafka")
        @SuppressWarnings("unchecked")
        public CompletionStage<Void> consume(Message<List<String>> batchMessage) {
            this.metadata.add(batchMessage.getMetadata(IncomingKafkaRecordBatchMetadata.class)
                    .orElseThrow(() -> new IllegalArgumentException("kafka batch metadata not found")));
            return batchMessage.ack();
        }

        public List<IncomingKafkaRecordBatchMetadata<String, String>> metadata() {
            return this.metadata;
        }

    }
}
