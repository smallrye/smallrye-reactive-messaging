package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.util.LinkedHashMap;
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
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

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

    @Test
    public void testFromKafkaToAppToKafkaForwardKey() {
        String topicIn = UUID.randomUUID().toString();
        LinkedHashMap<String, String> messages = new LinkedHashMap<>();
        usage.consumeStrings(topic, 10, 1, TimeUnit.MINUTES, null, messages::put);
        runApplication(getKafkaSinkConfigForAppProcessingDataForwardKey(topic, topicIn), MyAppForwardingKey.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(10, null, () -> {
            int c = count.getAndIncrement();
            return new ProducerRecord<>(topicIn, String.valueOf(c), c);
        });

        await().until(() -> messages.size() >= 10);

        assertThat(messages).containsKeys("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(messages).containsValues("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Test
    public void testFromKafkaToAppToKafkaForwardKeyReturningMessage() {
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topic, 10, 1, TimeUnit.MINUTES, null,
                (k, v) -> messages.add(entry(k, v)));
        runApplication(getKafkaSinkConfigForAppProcessingDataForwardKey(topic, topicIn),
                MyAppForwardingKeyReturningMessage.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(10, null, () -> {
            int c = count.getAndIncrement();
            return new ProducerRecord<>(topicIn, String.valueOf(c), c);
        });

        await().until(() -> messages.size() >= 10);

        assertThat(messages).extracting(Map.Entry::getKey)
                .containsExactly("even", "1", "even", "3", "even", "5", "even", "7", "even", "9");
        assertThat(messages).extracting(Map.Entry::getValue)
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @Test
    public void testFromKafkaToAppToKafkaForwardKeyIncomingRecordKeyNull() {
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topic, 10, 1, TimeUnit.MINUTES, null,
                (k, v) -> messages.add(entry(k, v)));
        runApplication(getKafkaSinkConfigForAppProcessingDataForwardKey(topic, topicIn),
                MyAppForwardingKeyReturningMessage.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(10, null, () -> {
            int c = count.getAndIncrement();
            return new ProducerRecord<>(topicIn, null, c);
        });

        await().until(() -> messages.size() >= 10);

        assertThat(messages).extracting(Map.Entry::getKey)
                .containsExactly("even", "even", "even", "even", "even", "even", "even", "even", "even", "even");
        assertThat(messages).extracting(Map.Entry::getValue)
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testFromKafkaToAppWithMetadata() {
        runApplication(getKafkaSourceConfigForMyAppWithKafkaMetadata(topic), MyAppWithKafkaMetadata.class);

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
            // TODO Import normally once the deprecated copy in this package has gone
            if (object instanceof io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata) {
                io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata incomingMetadata = (io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata) object;
                assertThat(incomingMetadata.getKey()).isEqualTo("a-key");
                assertThat(incomingMetadata.getTopic()).isEqualTo(topic);
                foundMetadata.compareAndSet(false, true);
            }
        }
        assertThat(foundMetadata.get()).isTrue();
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        return kafkaConfig("mp.messaging.outgoing.kafka")
                .put("value.serializer", StringSerializer.class.getName())
                .put("topic", "should-not-be-used");
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppProcessingData(String topicOut, String topicIn) {
        return kafkaConfig("mp.messaging.outgoing.kafka")
                .put("value.serializer", StringSerializer.class.getName())
                .put("topic", topicOut)
                .withPrefix("mp.messaging.incoming.source")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("graceful-shutdown", false)
                .put("topic", topicIn)
                .put("commit-strategy", "latest");
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForAppProcessingDataForwardKey(String topicOut, String topicIn) {
        return kafkaConfig("mp.messaging.outgoing.kafka")
                .put("value.serializer", StringSerializer.class.getName())
                .put("key.serializer", StringSerializer.class.getName())
                .put("topic", topicOut)
                .put("propagate-record-key", true)
                .put("key", "even")
                .withPrefix("mp.messaging.incoming.source")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("graceful-shutdown", false)
                .put("topic", topicIn)
                .put("commit-strategy", "latest");
    }

    private KafkaMapBasedConfig getKafkaSourceConfigForMyAppWithKafkaMetadata(String topic) {
        return kafkaConfig("mp.messaging.incoming.kafka")
                .put("value.deserializer", IntegerDeserializer.class.getName())
                .put("key.deserializer", StringDeserializer.class.getName())
                .put("auto.offset.reset", "earliest")
                .put("topic", topic)
                .put("commit-strategy", "latest");
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

    @ApplicationScoped
    public static class MyAppForwardingKey {

        @Incoming("source")
        @Outgoing("kafka")
        public String processMessage(Message<Integer> input) {
            return Integer.toString(input.getPayload() + 1);
        }

    }

    @ApplicationScoped
    public static class MyAppForwardingKeyReturningMessage {

        @Incoming("source")
        @Outgoing("kafka")
        public Message<String> processMessage(Message<Integer> input) {
            Integer payload = input.getPayload();
            if (payload % 2 != 0) {
                // if not even then return the input message with new payload
                return KafkaRecord.from(input).withPayload(Integer.toString(payload + 1));
            } else {
                // if even return new record with null key
                return KafkaRecord.of(null, Integer.toString(payload + 1));
            }
        }

    }

}
