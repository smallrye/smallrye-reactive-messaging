package io.smallrye.reactive.messaging.kafka.config;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ClientCustomizer;
import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;

/**
 * Test configuration interceptor
 */
public class ClientCustomizerTest extends KafkaCompanionTestBase {

    @Test
    public void testConfigInterceptor() {
        addBeans(MyClientCustomizer.class, OtherClientCustomizer.class);

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "key", "hello"), 5)
                .awaitCompletion();

        KafkaConsumer consumer = runApplication(kafkaConfig("mp.messaging.incoming.in")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with("topic", topic), KafkaConsumer.class);

        await().until(() -> isReady() && isAlive());

        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> consumer.getMessages().size() == 5);
    }

    @ApplicationScoped
    public static class KafkaConsumer {

        private final List<String> messages = new CopyOnWriteArrayList<>();

        @Incoming("in")
        public void consume(String incoming) {
            messages.add(incoming);
        }

        public List<String> getMessages() {
            return messages;
        }

    }

    @ApplicationScoped
    public static class MyClientCustomizer implements ClientCustomizer<Map<String, Object>> {

        @Override
        public Map<String, Object> customize(String channel, Config channelConfig, Map<String, Object> config) {
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return config;
        }
    }

    @ApplicationScoped
    public static class OtherClientCustomizer implements ClientCustomizer<String> {

        @Override
        public String customize(String channel, Config channelConfig, String config) {
            Assertions.fail("Should not be called");
            return "";
        }
    }

}
