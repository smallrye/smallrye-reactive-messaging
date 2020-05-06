package io.smallrye.reactive.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.awaitility.Awaitility.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.After;
import org.junit.Test;

import io.smallrye.config.SmallRyeConfigProviderResolver;

/**
 * Test that the config can be retrieved from a Map produced using the {@code default-kafka-broker} name
 */
public class DefaultConfigTest extends KafkaTestBase {

    private WeldContainer container;

    @After
    public void cleanup() {
        if (container != null) {
            container.close();
        }
        // Release the config objects
        SmallRyeConfigProviderResolver.instance().releaseConfig(ConfigProvider.getConfig());
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

    private MapBasedConfig getKafkaSinkConfigForMyAppProcessingData() {
        String prefix = "mp.messaging.outgoing.kafka.";
        Map<String, Object> config = new HashMap<>();
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "topic", "some-other-topic");

        prefix = "mp.messaging.incoming.source.";
        config.put(prefix + "connector", KafkaConnector.CONNECTOR_NAME);
        config.put(prefix + "topic", "some-topic");

        config.put("kafka.value.serializer", StringSerializer.class.getName());
        config.put("kafka.value.deserializer", IntegerDeserializer.class.getName());
        config.put("kafka.key.deserializer", StringDeserializer.class.getName());

        return new MapBasedConfig(config);
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

            return properties;
        }
    }

}
