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

import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.base.KafkaTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

/**
 * Test configuration where the channel names uses "dots"
 */
public class ConfigWithDotsTest extends KafkaTestBase {

    @Test
    public void testConfigurationWithDots() throws InterruptedException {

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.\"tc.payments.domain_event.job_created\".graceful-shutdown", false)
                .with("mp.messaging.incoming.\"tc.payments.domain_event.job_created\".bootstrap.servers",
                        usage.getBootstrapServers())
                .with("mp.messaging.incoming.\"tc.payments.domain_event.job_created\".connector",
                        KafkaConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.\"tc.payments.domain_event.job_created\".value.deserializer",
                        StringDeserializer.class.getName())
                .with("mp.messaging.incoming.\"tc.payments.domain_event.job_created\"."
                        + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .with("mp.messaging.incoming.\"tc.payments.domain_event.job_created\".topic", topic);

        KafkaConsumer consumer = runApplication(config, KafkaConsumer.class);

        await().until(() -> isReady() && isAlive());
        CountDownLatch latch = new CountDownLatch(1);
        usage.produceStrings(5, latch::countDown, () -> new ProducerRecord<>(topic, "key", "hello"));
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

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
