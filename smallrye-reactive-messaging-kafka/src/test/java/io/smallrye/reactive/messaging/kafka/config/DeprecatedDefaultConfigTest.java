package io.smallrye.reactive.messaging.kafka.config;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

// this entire file should be removed when support for the `@Named` annotation is removed

/**
 * Test that the config can be retrieved from a Map produced using the {@code default-kafka-broker} name
 */
public class DeprecatedDefaultConfigTest extends KafkaCompanionTestBase {

    @Test
    public void testFromKafkaToAppToKafka() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        ConsumerTask<String, String> records = companion.consumeStrings().fromTopics(topicOut, 10, Duration.ofMinutes(1));
        runApplication(getKafkaSinkConfigForMyAppProcessingData(topicOut, topicIn), MyAppProcessingData.class);

        companion.produceIntegers().usingGenerator(i -> new ProducerRecord<>(topicIn, "a-key", i), 10);

        await().until(() -> records.getRecords().size() >= 10);
        assertThat(records.getRecords()).allSatisfy(record -> {
            assertThat(record.key()).isEqualTo("my-key");
            assertThat(record.value()).isNotNull();
        });
    }

    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData(String topicOut, String topicIn) {
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

}
