package io.smallrye.reactive.messaging.kafka.config;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;

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

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Test that the config can be retrieved from a Map produced using the {@code default-kafka-broker} name as well as
 * channel specific configuration.
 */
public class DefaultConfigTest extends KafkaCompanionTestBase {

    @Test
    public void testFromKafkaToAppToKafkaWithIdentifiedConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                .with("mp.messaging.incoming.source.kafka-configuration", "my-kafka-broker")
                .with("mp.messaging.outgoing.kafka.kafka-configuration", "my-kafka-broker"),
                MyAppProcessingDataWithIdentifiedKafkaConfig.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafkaWithNotMatchingNamedConfigOnIncoming() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        try {
            runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                    .with("mp.messaging.incoming.source.kafka-configuration", "my-kafka-broker-boom")
                    .with("mp.messaging.outgoing.kafka.kafka-configuration", "my-kafka-broker"),
                    MyAppProcessingDataWithIdentifiedKafkaConfig.class);
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
                    .with("mp.messaging.incoming.source.kafka-configuration", "my-kafka-broker")
                    .with("mp.messaging.outgoing.kafka.kafka-configuration", "my-kafka-broker-boom"),
                    MyAppProcessingDataWithIdentifiedKafkaConfig.class);
            fail("Exception expected");
        } catch (DeploymentException e) {
            assertThat(e).hasMessageContaining("my-kafka-broker-boom");
        }
    }

    @Test
    public void testFromKafkaToAppToKafkaWithNamedConfigOnConnectorConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithNamedConfig(topicOut, topicIn)
                .with("mp.messaging.connector.smallrye-kafka.kafka-configuration", "my-kafka-broker"),
                MyAppProcessingDataWithIdentifiedKafkaConfig.class);

        AtomicInteger count = new AtomicInteger();
        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafka() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithDefaultConfig(topicOut, topicIn), MyAppProcessingData.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafkaWithNamed() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithDefaultConfig(topicOut, topicIn), MyAppProcessingDataNamed.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafkaWithNamedAndDefaultConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithNamedAndDefaultConfig(topicOut, topicIn)
                .with("mp.messaging.incoming.source.kafka-configuration", "my-kafka-broker")
                .with("mp.messaging.outgoing.kafka.kafka-configuration", "my-kafka-broker"),
                MyAppProcessingDataWithNamedAndDefaultConfig.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafkaWithChannelAndDefaultConfig() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithChannelAndDefaultConfig(topicOut, topicIn),
                MyAppProcessingDataWithChannelAndDefaultConfig.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    @Test
    public void testFromKafkaToAppToKafkaWithChannel() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaConfigWithChannel(topicOut, topicIn),
                MyAppProcessingDataWithChannel.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    private MapBasedConfig getKafkaConfigWithDefaultConfig(String topicOut, String topicIn) {
        MapBasedConfig config = new MapBasedConfig();
        config.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        config.put("mp.messaging.outgoing.kafka.topic", topicOut);

        config.put("mp.messaging.incoming.source.topic", topicIn);
        config.put("mp.messaging.incoming.source.graceful-shutdown", false);
        config.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        config.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        config.put("mp.messaging.incoming.source.commit-strategy", "latest");

        config.put("kafka.bootstrap.servers", companion.getBootstrapServers());
        config.put("kafka.value.serializer", StringSerializer.class.getName());
        config.put("kafka.value.deserializer", IntegerDeserializer.class.getName());
        config.put("kafka.key.deserializer", StringDeserializer.class.getName());

        return config;
    }

    private MapBasedConfig getKafkaConfigWithNamedConfig(String topicOut, String topicIn) {
        MapBasedConfig config = new MapBasedConfig();
        config.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        config.put("mp.messaging.outgoing.kafka.topic", topicOut);

        config.put("mp.messaging.incoming.source.topic", topicIn);
        config.put("mp.messaging.incoming.source.graceful-shutdown", false);
        config.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        config.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        config.put("mp.messaging.incoming.source.commit-strategy", "latest");

        config.put("my-kafka-broker.bootstrap.servers", companion.getBootstrapServers());
        config.put("my-kafka-broker.value.serializer", StringSerializer.class.getName());
        config.put("my-kafka-broker.value.deserializer", IntegerDeserializer.class.getName());
        config.put("my-kafka-broker.key.deserializer", StringDeserializer.class.getName());

        return config;
    }

    private MapBasedConfig getKafkaConfigWithNamedAndDefaultConfig(String topicOut, String topicIn) {
        MapBasedConfig config = new MapBasedConfig();
        config.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        config.put("mp.messaging.outgoing.kafka.topic", topicOut);

        config.put("mp.messaging.incoming.source.topic", topicIn);
        config.put("mp.messaging.incoming.source.graceful-shutdown", false);
        config.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        config.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        config.put("mp.messaging.incoming.source.commit-strategy", "latest");

        config.put("my-kafka-broker.bootstrap.servers", companion.getBootstrapServers());
        config.put("my-kafka-broker.value.serializer", StringSerializer.class.getName());
        config.put("kafka.value.deserializer", IntegerDeserializer.class.getName());
        config.put("kafka.key.deserializer", StringDeserializer.class.getName());
        // Overridden - illegal value on purpose
        config.put("kafka.value.serializer", Integer.class.getName());

        return config;
    }

    private MapBasedConfig getKafkaConfigWithChannelAndDefaultConfig(String topicOut, String topicIn) {
        MapBasedConfig config = new MapBasedConfig();
        config.put("mp.messaging.outgoing.kafka.connector", CONNECTOR_NAME);
        config.put("mp.messaging.outgoing.kafka.topic", topicOut);

        config.put("mp.messaging.incoming.source.topic", topicIn);
        config.put("mp.messaging.incoming.source.graceful-shutdown", false);
        config.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        config.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        config.put("mp.messaging.incoming.source.commit-strategy", "latest");

        config.put("kafka.bootstrap.servers", companion.getBootstrapServers());
        config.put("kafka.value.serializer", StringSerializer.class.getName());
        config.put("source.value.deserializer", IntegerDeserializer.class.getName());
        config.put("kafka.key.deserializer", StringDeserializer.class.getName());
        // Overridden - illegal value on purpose
        config.put("kafka.value.deserializer", Integer.class.getName());

        return config;
    }

    private MapBasedConfig getKafkaConfigWithChannel(String topicOut, String topicIn) {
        MapBasedConfig config = new MapBasedConfig();
        config.put("mp.messaging.outgoing.my-kafka.connector", CONNECTOR_NAME);
        config.put("mp.messaging.outgoing.my-kafka.topic", topicOut);

        config.put("mp.messaging.incoming.source.topic", topicIn);
        config.put("mp.messaging.incoming.source.graceful-shutdown", false);
        config.put("mp.messaging.incoming.source.connector", CONNECTOR_NAME);
        config.put("mp.messaging.incoming.source.auto.offset.reset", "earliest");
        config.put("mp.messaging.incoming.source.commit-strategy", "latest");

        config.put("my-kafka.bootstrap.servers", companion.getBootstrapServers());
        config.put("my-kafka.value.serializer", StringSerializer.class.getName());
        config.put("source.key.deserializer", StringDeserializer.class.getName());
        config.put("source.bootstrap.servers", companion.getBootstrapServers());
        config.put("source.value.deserializer", IntegerDeserializer.class.getName());

        return config;
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
        @Identifier("default-kafka-broker")
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
    public static class MyAppProcessingDataNamed {

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
    public static class MyAppProcessingDataWithIdentifiedKafkaConfig {

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
        @Identifier("my-kafka-broker")
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
        @Identifier("my-kafka-broker")
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
        @Identifier("default-kafka-broker")
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
        @Identifier("source")
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
        @Identifier("default-kafka-broker")
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
        @Identifier("source")
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
        @Identifier("my-kafka")
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
