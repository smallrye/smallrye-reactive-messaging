package io.smallrye.reactive.messaging.kafka.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ConfigOverrideFromEnvTest extends KafkaTestBase {

    final static String TOPIC = "ConfigOverrideFromEnvTest-Topic";

    @Test
    @SetEnvironmentVariable(key = "MP_MESSAGING_INCOMING_MY_CHANNEL_TOPIC", value = TOPIC)
    @DisabledOnJre(value = JRE.JAVA_17, disabledReason = "Environment cannot be modified on Java 17")
    public void testOverridingTopicFromEnv() throws InterruptedException {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.my-channel.graceful-shutdown", false)
                .with("mp.messaging.incoming.my-channel.topic", "should not be used")
                .with("mp.messaging.incoming.my-channel.bootstrap.servers", usage.getBootstrapServers())
                .with("mp.messaging.incoming.my-channel.connector",
                        KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.my-channel.value.deserializer",
                        StringDeserializer.class.getName())
                .with("mp.messaging.incoming.my-channel."
                        + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer consumer = runApplication(config, Consumer.class);
        await().until(() -> isReady() && isAlive());

        CountDownLatch latch = new CountDownLatch(1);
        usage.produceStrings(5, latch::countDown, () -> new ProducerRecord<>(TOPIC, "key", "hello"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

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
