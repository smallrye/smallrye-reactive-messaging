package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.reactivex.Flowable;
import io.smallrye.config.SmallRyeConfigProviderResolver;

public class MetadataPropagationTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
    }

    @Test
    public void testFromAppToKafka() {
        KafkaUsage usage = new KafkaUsage();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings("some-topic", 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        deploy(getKafkaSinkConfigForMyAppGeneratingData(), MyAppGeneratingData.class);

        await().until(() -> messages.size() == 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getKey()).isEqualTo("my-key");
            assertThat(entry.getValue()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafka() {
        KafkaUsage usage = new KafkaUsage();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings("some-other-topic", 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        deploy(getKafkaSinkConfigForMyAppProcessingData(), MyAppProcessingData.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(100, null,
                () -> new ProducerRecord<>("some-topic", "a-key", count.getAndIncrement()));

        await().until(() -> messages.size() >= 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getKey()).isEqualTo("my-key");
            assertThat(entry.getValue()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppWithMetadata() {
        KafkaUsage usage = new KafkaUsage();
        deploy(getKafkaSinkConfigForMyAppWithKafkaMetadata(), MyAppWithKafkaMetadata.class);

        AtomicInteger value = new AtomicInteger();
        usage.produceIntegers(100, null,
                () -> new ProducerRecord<>("metadata-topic", "a-key", value.getAndIncrement()));

        MyAppWithKafkaMetadata bean = container.getBeanManager().createInstance().select(MyAppWithKafkaMetadata.class).get();
        await().atMost(2, TimeUnit.MINUTES).until(() -> bean.list().size() >= 10);
        assertThat(bean.list()).contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        assertThat(bean.getMetadata()).isNotNull();
        assertThat(bean.getMetadata()).contains(bean.getOriginal());
        AtomicBoolean foundMetadata = new AtomicBoolean(false);
        for (Object object : bean.getMetadata()) {
            if (object instanceof IncomingKafkaRecordMetadata) {
                IncomingKafkaRecordMetadata incomingMetadata = (IncomingKafkaRecordMetadata) object;
                assertThat(incomingMetadata.getKey()).isEqualTo("a-key");
                assertThat(incomingMetadata.getTopic()).isEqualTo("metadata-topic");
                foundMetadata.compareAndSet(false, true);
            }
        }
        assertThat(foundMetadata.get()).isTrue();
    }

    private <T> void deploy(MapBasedConfig config, Class<T> clazz) {
        if (config != null) {
            config.write();
        } else {
            MapBasedConfig.clear();
        }

        Weld weld = baseWeld();
        weld.addBeanClass(clazz);

        container = weld.initialize();
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppGeneratingData() {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "topic", "should-not-be-used");
        return new MapBasedConfig(config);
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData() {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.serializer", StringSerializer.class.getName());
        config.put(prefix + "topic", "some-other-topic");

        prefix = "mp.messaging.incoming.source.";
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "key.deserializer", StringDeserializer.class.getName());
        config.put(prefix + "topic", "some-topic");
        config.put(prefix + "commit-strategy", "latest");

        return new MapBasedConfig(config);
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppWithKafkaMetadata() {
        String prefix = "mp.messaging.incoming.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "value.deserializer", IntegerDeserializer.class.getName());
        config.put(prefix + "key.deserializer", StringDeserializer.class.getName());
        config.put(prefix + "auto.offset.reset", "earliest");
        config.put(prefix + "topic", "metadata-topic");
        config.put(prefix + "commit-strategy", "latest");
        return new MapBasedConfig(config);
    }

    @SuppressWarnings("deprecation")
    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Outgoing("source")
        public Flowable<Integer> source() {
            return Flowable.range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaMessage.of("some-topic", "my-key", input.getPayload());
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
