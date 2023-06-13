package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class ProducerConsumerTest extends WeldTestBase {

    @Test
    void testProducerConsumer() {
        ProducerConsumerApp app = runApplication(config(), ProducerConsumerApp.class);

        await().untilAsserted(() -> assertThat(app.received()).contains("1", "2", "3", "4", "5"));
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.pulsar.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.pulsar.serviceUrl", serviceUrl)
                .with("mp.messaging.outgoing.pulsar.topic", topic)
                .with("mp.messaging.outgoing.pulsar.schema", "STRING")
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "STRING");
    }

    @ApplicationScoped
    public static class ProducerConsumerApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Outgoing("pulsar")
        public Multi<String> produce() {
            return Multi.createFrom().items("1", "2", "3", "4", "5");
        }

        @Incoming("data")
        public void produce(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }
}
