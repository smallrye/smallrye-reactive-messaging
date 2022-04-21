package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.converters.ConsumerRecordConverter;

/**
 * Duplicate of {@link ProducerRecordTest} - delete once we remove the legacy
 * {@link io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata}
 */
public class ProducerRecordWithLegacyMetadataTest extends KafkaCompanionTestBase {
    private static final String TOPIC_NAME_BASE = "ProducerRecord-" + UUID.randomUUID() + "-";

    @Test
    public void test() {
        for (int i = 0; i < 10; i++) {
            companion.topics().createAndWait(TOPIC_NAME_BASE + i, 1);
        }

        addBeans(ConsumerRecordConverter.class);
        runApplication(config(), MyApp.class);

        MyApp bean = get(MyApp.class);

        await().until(() -> bean.received().size() >= 500);
        List<ConsumerRecord<String, String>> messages = bean.received();

        assertThat(messages).allSatisfy(consumerRecord -> {
            assertThat(consumerRecord.key()).startsWith("key-");
            assertThat(consumerRecord.value()).startsWith("value-");
            assertThat(consumerRecord.topic()).startsWith(TOPIC_NAME_BASE);

            assertThat(consumerRecord.headers()).allSatisfy(header -> {
                assertThat(header.key()).startsWith("my-header-");
                assertThat(new String(header.value(), StandardCharsets.UTF_8)).startsWith("my-header-value-");
            });
            assertThat(consumerRecord.headers()).noneSatisfy(header -> {
                assertThat(header.key()).startsWith("my-other-header-");
                assertThat(new String(header.value(), StandardCharsets.UTF_8)).startsWith("my-other-header-value-");
            });
        });

        Set<String> topics = messages.stream()
                .map(ConsumerRecord::topic)
                .collect(Collectors.toSet());
        for (int i = 0; i < 10; i++) {
            assertThat(topics).contains(TOPIC_NAME_BASE + i);
        }
    }

    private KafkaMapBasedConfig config() {
        return kafkaConfig("mp.messaging.outgoing.generated-producer")
                .put("topic", "nonexistent-topic")
                .put("key.serializer", StringSerializer.class.getName())
                .put("value.serializer", StringSerializer.class.getName())

                .withPrefix("mp.messaging.incoming.generated-consumer")
                .put("topic", TOPIC_NAME_BASE + ".+")
                .put("pattern", true)
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("value.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest");
    }

    @ApplicationScoped
    public static class MyApp {
        private final List<ConsumerRecord<String, String>> received = new CopyOnWriteArrayList<>();

        @SuppressWarnings("deprecation")
        @Outgoing("generated-producer")
        public Multi<Message<ProducerRecord<String, String>>> produce() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(10)).map(tick -> {
                int id = tick.intValue();
                int topicId = id % 10;

                Headers headersToBeUsed = new RecordHeaders()
                        .add("my-header-" + id, ("my-header-value-" + id).getBytes(StandardCharsets.UTF_8));

                Headers headersToBeLost = new RecordHeaders()
                        .add("my-other-header-" + id, ("my-other-header-value-" + id).getBytes(StandardCharsets.UTF_8));

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME_BASE + topicId, null,
                        "key-" + id, "value-" + id, headersToBeUsed);

                return Message.of(record)
                        .addMetadata(OutgoingKafkaRecordMetadata.<String> builder()
                                .withTopic("nonexistent-topic-" + id)
                                .withHeaders(headersToBeLost)
                                .build());
            });
        }

        @Incoming("generated-consumer")
        public void consume(ConsumerRecord<String, String> msg) {
            received.add(msg);
        }

        public List<ConsumerRecord<String, String>> received() {
            return received;
        }
    }
}
