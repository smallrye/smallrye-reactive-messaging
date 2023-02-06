package io.smallrye.reactive.messaging.kafka.config;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Test configuration where the channel names uses "dots"
 */
public class ConfigWithDotsTest extends KafkaCompanionTestBase {

    @Test
    public void testConfigurationWithDots() {

        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.\"tc.payments.domain_event.job_created\"")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with("topic", topic);

        KafkaConsumer consumer = runApplication(config, KafkaConsumer.class);

        await().until(() -> isReady() && isAlive());
        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(topic, "key", "hello"), 5)
                .awaitCompletion();

        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> consumer.getMessages().size() == 5);
    }

    @ApplicationScoped
    public static class KafkaConsumer {

        private final List<String> messages = new CopyOnWriteArrayList<>();

        @Incoming("tc.payments.domain_event.job_created")
        public void consume(String incoming) {
            messages.add(incoming);
        }

        public List<String> getMessages() {
            return messages;
        }

    }

}
