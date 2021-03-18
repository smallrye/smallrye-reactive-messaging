package io.smallrye.reactive.messaging.kafka;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.exceptions.DeploymentException;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

/**
 * Test that the config can be retrieved from a Map produced using the {@code default-kafka-broker} name
 */
public class DefaultConfigTest extends KafkaTestBase {

    @Test
    public void testFromKafkaToAppToKafkaWithNamedConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                .with("mp.messaging.incoming.source.kafka-configuration-name", "my-kafka-broker")
                .with("mp.messaging.outgoing.kafka.kafka-configuration-name", "my-kafka-broker"),
                MyAppProcessingDataWithNamedConfig.class);

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
    public void testFromKafkaToAppToKafkaWithNotMatchingNamedConfigOnIncoming() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        try {
            runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                    .with("mp.messaging.incoming.source.kafka-configuration-name", "my-kafka-broker-boom")
                    .with("mp.messaging.outgoing.kafka.kafka-configuration-name", "my-kafka-broker"),
                    MyAppProcessingDataWithNamedConfig.class);
            fail("Exception expected");
        } catch (DeploymentException e) {
            assertThat(e).hasMessageContaining("my-kafka-broker-boom");
        }
    }

    @Test
    public void testFromKafkaToAppToKafkaWithNotMatchingNamedConfigOnOutgoing() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        try {
            runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                    .with("mp.messaging.incoming.source.kafka-configuration-name", "my-kafka-broker")
                    .with("mp.messaging.outgoing.kafka.kafka-configuration-name", "my-kafka-broker-boom"),
                    MyAppProcessingDataWithNamedConfig.class);
            fail("Exception expected");
        } catch (DeploymentException e) {
            assertThat(e).hasMessageContaining("my-kafka-broker-boom");
        }
    }

    @Test
    public void testFromKafkaToAppToKafkaWithNamedConfigOnConnectorConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                .with("mp.messaging.connector.smallrye-kafka.kafka-configuration-name", "my-kafka-broker"),
                MyAppProcessingDataWithNamedConfig.class);

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
    public void testFromKafkaToAppToKafka() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaConfigWithDefaultConfig(topicOut, topicIn), MyAppProcessingData.class);

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
    public void testFromKafkaToAppToKafkaWithNamedAndDefaultConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaConfigWithNamedAndDefaultConfig(topicOut, topicIn)
                .with("mp.messaging.incoming.source.kafka-configuration-name", "my-kafka-broker")
                .with("mp.messaging.outgoing.kafka.kafka-configuration-name", "my-kafka-broker"),
                MyAppProcessingDataWithNamedAndDefaultConfig.class);

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
    public void testFromKafkaToAppToKafkaWithChannelAndDefaultConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaConfigWithChannelAndDefaultConfig(topicOut, topicIn),
                MyAppProcessingDataWithChannelAndDefaultConfig.class);

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
    public void testFromKafkaToAppToKafkaWithChannel() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaConfigWithChannel(topicOut, topicIn),
                MyAppProcessingDataWithChannel.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topicIn, "a-key", count.getAndIncrement()));

        await().until(() -> messages.size() >= 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getKey()).isEqualTo("my-key");
            assertThat(entry.getValue()).isNotNull();
        });
    }

    private KafkaMapBasedConfig getKafkaConfigWithDefaultConfig(String topicOut, String topicIn) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder();
        builder.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.kafka.topic", topicOut);

        builder.put("mp.messaging.incoming.source.topic", topicIn);
        builder.put("mp.messaging.incoming.source.graceful-shutdown", false);
        builder.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.source.commit-strategy", "latest");

        builder.put("kafka.bootstrap.servers", getBootstrapServers());
        builder.put("kafka.value.serializer", StringSerializer.class.getName());
        builder.put("kafka.value.deserializer", IntegerDeserializer.class.getName());
        builder.put("kafka.key.deserializer", StringDeserializer.class.getName());

        return builder.build();
    }

    private KafkaMapBasedConfig getKafkaConfigWithNamedConfig(String topicOut, String topicIn) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder();
        builder.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.kafka.topic", topicOut);

        builder.put("mp.messaging.incoming.source.topic", topicIn);
        builder.put("mp.messaging.incoming.source.graceful-shutdown", false);
        builder.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.source.commit-strategy", "latest");

        builder.put("my-kafka-broker.bootstrap.servers", getBootstrapServers());
        builder.put("my-kafka-broker.value.serializer", StringSerializer.class.getName());
        builder.put("my-kafka-broker.value.deserializer", IntegerDeserializer.class.getName());
        builder.put("my-kafka-broker.key.deserializer", StringDeserializer.class.getName());

        return builder.build();
    }

    private KafkaMapBasedConfig getKafkaConfigWithNamedAndDefaultConfig(String topicOut, String topicIn) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder();
        builder.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.kafka.topic", topicOut);

        builder.put("mp.messaging.incoming.source.topic", topicIn);
        builder.put("mp.messaging.incoming.source.graceful-shutdown", false);
        builder.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.source.commit-strategy", "latest");

        builder.put("my-kafka-broker.bootstrap.servers", getBootstrapServers());
        builder.put("my-kafka-broker.value.serializer", StringSerializer.class.getName());
        builder.put("kafka.value.deserializer", IntegerDeserializer.class.getName());
        builder.put("kafka.key.deserializer", StringDeserializer.class.getName());
        // Overridden - illegal value on purpose
        builder.put("kafka.value.serializer", Integer.class.getName());

        return builder.build();
    }

    private KafkaMapBasedConfig getKafkaConfigWithChannelAndDefaultConfig(String topicOut, String topicIn) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder();
        builder.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.kafka.topic", topicOut);

        builder.put("mp.messaging.incoming.source.topic", topicIn);
        builder.put("mp.messaging.incoming.source.graceful-shutdown", false);
        builder.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.source.commit-strategy", "latest");

        builder.put("kafka.bootstrap.servers", getBootstrapServers());
        builder.put("kafka.value.serializer", StringSerializer.class.getName());
        builder.put("source.value.deserializer", IntegerDeserializer.class.getName());
        builder.put("kafka.key.deserializer", StringDeserializer.class.getName());
        // Overridden - illegal value on purpose
        builder.put("kafka.value.deserializer", Integer.class.getName());

        return builder.build();
    }

    private KafkaMapBasedConfig getKafkaConfigWithChannel(String topicOut, String topicIn) {
        KafkaMapBasedConfig.Builder builder = KafkaMapBasedConfig.builder();
        builder.put("mp.messaging.outgoing.my-kafka.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.outgoing.my-kafka.topic", topicOut);

        builder.put("mp.messaging.incoming.source.topic", topicIn);
        builder.put("mp.messaging.incoming.source.graceful-shutdown", false);
        builder.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        builder.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        builder.put("mp.messaging.incoming.source.commit-strategy", "latest");

        builder.put("my-kafka.bootstrap.servers", getBootstrapServers());
        builder.put("my-kafka.value.serializer", StringSerializer.class.getName());
        builder.put("source.key.deserializer", StringDeserializer.class.getName());
        builder.put("source.bootstrap.servers", getBootstrapServers());
        builder.put("source.value.deserializer", IntegerDeserializer.class.getName());

        return builder.build();
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.of("my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }

        @Inject
        Config config;

        @Produces
        @ApplicationScoped
        @Named("default-kafka-broker")
        public Map<String, Object> createKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("kafka"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("kafka".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]", ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingDataWithNamedConfig {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.of("my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }

        @Inject
        Config config;

        @Produces
        @ApplicationScoped
        @Named("my-kafka-broker")
        public Map<String, Object> createKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("my-kafka-broker"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("my-kafka-broker".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingDataWithNamedAndDefaultConfig {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.of("my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }

        @Inject
        Config config;

        @Produces
        @ApplicationScoped
        @Named("my-kafka-broker")
        public Map<String, Object> createKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("my-kafka-broker"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("my-kafka-broker".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }

        @Produces
        @ApplicationScoped
        @Named("default-kafka-broker")
        public Map<String, Object> createDefaultKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("kafka"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("kafka".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingDataWithChannelAndDefaultConfig {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.of("my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }

        @Inject
        Config config;

        @Produces
        @ApplicationScoped
        @Named("source")
        public Map<String, Object> createChannelKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("source"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("source".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }

        @Produces
        @ApplicationScoped
        @Named("default-kafka-broker")
        public Map<String, Object> createDefaultKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("kafka"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("kafka".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingDataWithChannel {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            return KafkaRecord.of("my-key", input.getPayload());
        }

        @Incoming("p1")
        @Outgoing("my-kafka")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }

        @Inject
        Config config;

        @Produces
        @ApplicationScoped
        @Named("source")
        public Map<String, Object> createChannelKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("source"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("source".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }

        @Produces
        @ApplicationScoped
        @Named("my-kafka")
        public Map<String, Object> createDefaultKafkaRuntimeConfig() {
            Map<String, Object> properties = new HashMap<>();

            StreamSupport
                    .stream(config.getPropertyNames().spliterator(), false)
                    .map(String::toLowerCase)
                    .filter(name -> name.startsWith("my-kafka"))
                    .distinct()
                    .sorted()
                    .forEach(name -> {
                        final String key = name.substring("my-kafka".length() + 1).toLowerCase().replaceAll("[^a-z0-9.]",
                                ".");
                        final String value = config.getOptionalValue(name, String.class).orElse("");
                        properties.put(key, value);
                    });
            // Here to verify that passing a boolean does not trigger an error.
            properties.put("some-boolean", true);
            return properties;
        }
    }

}
