package io.smallrye.reactive.messaging.kafka.companion.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;

import io.smallrye.reactive.messaging.kafka.companion.KafkaCompanion;

public class KafkaRestartTest {

    @Test
    void testRestartedBroker() {
        try (GenericContainer<?> kafkaContainer = KafkaBrokerExtension.createKafkaContainer()) {
            kafkaContainer.start();
            await().until(kafkaContainer::isRunning);
            String bootstrapServers = KafkaBrokerExtension.getBootstrapServers(kafkaContainer);
            try (KafkaCompanion companion = new KafkaCompanion(bootstrapServers)) {
                companion.produceStrings()
                        .fromRecords(new ProducerRecord<>("topic", "1"))
                        .awaitCompletion();

                GenericContainer<?> restarted = KafkaBrokerExtension.restart(kafkaContainer, 2);

                assertThat(KafkaBrokerExtension.getBootstrapServers(restarted)).isEqualTo(bootstrapServers);

                companion.produceStrings()
                        .fromRecords(new ProducerRecord<>("topic", "1"))
                        .awaitCompletion();
            }
        }
    }
}
