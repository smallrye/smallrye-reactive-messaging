package io.smallrye.reactive.messaging.kafka;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.kafka.base.MapBasedConfig;

public class MetadataPropagationTest extends KafkaTestBase {

    @Test
    public void testFromAppToKafka() {
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings("some-topic-testFromAppToKafka", 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaSinkConfigForMyAppGeneratingData(), MyAppGeneratingData.class);

        await().until(() -> messages.size() == 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getKey()).isEqualTo("my-key");
            assertThat(entry.getValue()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafka() {
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topic, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaSinkConfigForMyAppProcessingData(topic, topicIn), MyAppProcessingData.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topicIn, "a-key", count.getAndIncrement()));

        await().until(() -> messages.size() >= 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getKey()).isEqualTo("my-key");
            assertThat(entry.getValue()).isNotNull();
        });
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testFromKafkaToAppWithMetadata() {
        runApplication(getKafkaSinkConfigForMyAppWithKafkaMetadata(topic), MyAppWithKafkaMetadata.class);

        AtomicInteger value = new AtomicInteger();
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topic, "a-key", value.getAndIncrement()));

        MyAppWithKafkaMetadata bean = get(MyAppWithKafkaMetadata.class);
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(bean.getMetadata()).isNotNull();
        assertThat(bean.getMetadata()).contains(bean.getOriginal());
        AtomicBoolean foundMetadata = new AtomicBoolean(false);
        for (Object object : bean.getMetadata()) {
            if (object instanceof IncomingKafkaRecordMetadata) {
                IncomingKafkaRecordMetadata incomingMetadata = (IncomingKafkaRecordMetadata) object;
                assertThat(incomingMetadata.getKey()).isEqualTo("a-key");
                assertThat(incomingMetadata.getTopic()).isEqualTo(topic);
                foundMetadata.compareAndSet(false, true);
            }
        }
        assertThat(foundMetadata.get()).isTrue();
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.outgoing.kafka");
        builder.put("value.serializer", StringSerializer.class.getName());
        builder.put("topic", "should-not-be-used");
        return builder.build();
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData(String topicOut, String topicIn) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder();
        builder.put("mp.messaging.outgoing.kafka.value.serializer", StringSerializer.class.getName());
        builder.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.kafka.bootstrap.servers", getBootstrapServers());
        builder.put("mp.messaging.outgoing.kafka.topic", topicOut);

        builder.put("mp.messaging.incoming.source.value.deserializer", IntegerDeserializer.class.getName());
        builder.put("mp.messaging.incoming.source.key.deserializer", StringDeserializer.class.getName());
        builder.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.source.bootstrap.servers", getBootstrapServers());
        builder.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.source.topic", topicIn);
        builder.put("mp.messaging.incoming.source.commit-strategy", "latest");

        return builder.build();
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppWithKafkaMetadata(String topic) {
        MapBasedConfig.Builder builder = MapBasedConfig.builder("mp.messaging.incoming.kafka");
        builder.put("value.deserializer", IntegerDeserializer.class.getName());
        builder.put("key.deserializer", StringDeserializer.class.getName());
        builder.put("auto.offset.reset", "earliest");
        builder.put("topic", topic);
        builder.put("commit-strategy", "latest");
        return builder.build();
    }

    @SuppressWarnings("deprecation")
    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Outgoing("source")
        public Multi<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaMessage.of("some-topic-testFromAppToKafka", "my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

    @SuppressWarnings("deprecation")
    @ApplicationScoped
    public static class MyAppProcessingData {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaMessage.of("my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

    @ApplicationScoped
    public static class MyAppWithKafkaMetadata {
        private final List<Integer> received = new CopyOnWriteArrayList<>();
        private Metadata metadata;
        private final MetadataValue original = new MetadataValue("important");

        @Incoming("kafka")
        @Outgoing("source")
        public Message<Integer> source(Message<Integer> input) {
            return input.addMetadata(original);
        }

        @Incoming("source")
        @Outgoing("output")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.from(input);
        }

        @Incoming("output")
        public CompletionStage<Void> verify(Message<Integer> record) {
            received.add(record.getPayload());
            metadata = record.getMetadata();
            return CompletableFuture.completedFuture(null);
        }

        public List<Integer> list() {
            return received;
        }

        public Metadata getMetadata() {
            return metadata;
        }

        public MetadataValue getOriginal() {
            return original;
        }
    }

    public static class MetadataValue {
        private final String value;

        public MetadataValue(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
