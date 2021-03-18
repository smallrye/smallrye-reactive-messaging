package io.smallrye.reactive.messaging.kafka;

import static io.smallrye.reactive.messaging.kafka.KafkaConnector.CONNECTOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.base.KafkaMapBasedConfig;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;

/**
 * Test that the config can be retrieved from a Map produced using the {@code default-kafka-broker} name
 */
public class DefaultConfigTest extends KafkaTestBase {

    @Test
    public void testFromKafkaToAppToKafka() {
        String topicOut = UUID.randomUUID().toString();
        String topicIn = UUID.randomUUID().toString();
        List<Map.Entry<String, String>> messages = new CopyOnWriteArrayList<>();
        usage.consumeStrings(topicOut, 10, 1, TimeUnit.MINUTES, null,
                (key, value) -> messages.add(entry(key, value)));
        runApplication(getKafkaSinkConfigForMyAppProcessingData(topicOut, topicIn), MyAppProcessingData.class);

        AtomicInteger count = new AtomicInteger();
        usage.produceIntegers(10, null,
                () -> new ProducerRecord<>(topicIn, "a-key", count.getAndIncrement()));

        await().until(() -> messages.size() >= 10);
        assertThat(messages).allSatisfy(entry -> {
            assertThat(entry.getKey()).isEqualTo("my-key");
            assertThat(entry.getValue()).isNotNull();
        });
    }

    private KafkaMapBasedConfig getKafkaSinkConfigForMyAppProcessingData(String topicOut, String topicIn) {
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

}
