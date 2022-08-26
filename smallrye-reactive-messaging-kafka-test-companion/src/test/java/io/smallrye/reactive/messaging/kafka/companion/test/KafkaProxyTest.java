package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Latency;
import io.smallrye.reactive.messaging.kafka.companion.ConsumerTask;

public class KafkaProxyTest extends KafkaCompanionProxyTestBase {

    @Test
    void testProxyDisabled() {
        companion.produceIntegers().fromRecords(
                new ProducerRecord<>(topic, 1),
                new ProducerRecord<>(topic, 2),
                new ProducerRecord<>(topic, 3))
                .awaitCompletion();

        disableProxy();

        try (ConsumerTask<String, Integer> task = companion.consumeIntegers().fromTopics(topic, 3)) {
            assertThatThrownBy(() -> task.awaitRecords(1, Duration.ofSeconds(2))).isNotNull();
            enableProxy();
            task.awaitCompletion();
        }
    }

    @Test
    void testConnectionCut() {
        companion.produceIntegers().fromRecords(
                new ProducerRecord<>(topic, 1),
                new ProducerRecord<>(topic, 2),
                new ProducerRecord<>(topic, 3))
                .awaitCompletion();

        connectionCut(true);

        try (ConsumerTask<String, Integer> task = companion.consumeIntegers().fromTopics(topic, 3)) {
            assertThatThrownBy(() -> task.awaitRecords(1, Duration.ofSeconds(2))).isNotNull();
            connectionCut(false);
            task.awaitCompletion();
        }
    }

    @Test
    void testToxics() throws IOException {
        companion.produceIntegers().fromRecords(
                new ProducerRecord<>(topic, 1),
                new ProducerRecord<>(topic, 2),
                new ProducerRecord<>(topic, 3))
                .awaitCompletion();

        Latency latency = toxics().latency("latency", ToxicDirection.DOWNSTREAM, 300L);

        companion.consumeIntegers().fromTopics(topic, 3).awaitCompletion();

        latency.setLatency(0L);
        latency.remove();
        assertThat(toxics().getAll()).isEmpty();
    }
}
