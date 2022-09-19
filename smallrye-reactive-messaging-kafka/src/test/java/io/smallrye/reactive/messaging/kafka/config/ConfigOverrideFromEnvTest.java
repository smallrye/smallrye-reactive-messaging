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
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.reactive.messaging.kafka.base.KafkaCompanionTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ConfigOverrideFromEnvTest extends KafkaCompanionTestBase {

    final static String TOPIC = "ConfigOverrideFromEnvTest-Topic";

    @Test
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_MY_CHANNEL_TOPIC", value = TOPIC)
    @DisabledOnJre(value = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    public void testOverridingTopicFromEnv() throws InterruptedException {
        MapBasedConfig config = kafkaConfig("mp.messaging.incoming.my-channel")
                .with("graceful-shutdown", false)
                .with("topic", "should not be used")
                .with("value.deserializer", StringDeserializer.class.getName())
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer consumer = runApplication(config, Consumer.class);
        await().until(() -> isReady() && isAlive());

        companion.produceStrings().usingGenerator(i -> new ProducerRecord<>(TOPIC, "key", "hello"), 5)
                .awaitCompletion();

        await()
                .atMost(Duration.ofMinutes(1))
                .until(() -> consumer.list().size() == 5);
    }

    @ApplicationScoped
    public static class Consumer {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("my-channel")
        public void consume(String data) {
            list.add(data);
        }

        public List<String> list() {
            return list;
        }
    }

}
