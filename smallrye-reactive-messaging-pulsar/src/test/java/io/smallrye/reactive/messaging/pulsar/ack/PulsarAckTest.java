package io.smallrye.reactive.messaging.pulsar.ack;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.pulsar.PulsarConnector;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class PulsarAckTest extends WeldTestBase {

    public static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testAck() throws PulsarClientException {
        addBeans(PulsarMessageAck.Factory.class);
        // Run app
        ConsumingApp app = runApplication(config(), ConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
    }

    @Test
    void testCumulativeAck() throws PulsarClientException {
        addBeans(PulsarCumulativeAck.Factory.class);
        // Run app
        MapBasedConfig cumulative = config()
                .with("mp.messaging.incoming.data.ack-strategy", "cumulative");
        ConsumingApp app = runApplication(cumulative, ConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), NUMBER_OF_MESSAGES, i -> i);

        // Check for consumed messages in app
        await().atMost(Duration.ofSeconds(30)).until(() -> app.getResults().size() == NUMBER_OF_MESSAGES);
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32");
    }

    @ApplicationScoped
    public static class ConsumingApp {

        private final List<Integer> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Integer message) {
            results.add(message);
        }

        public List<Integer> getResults() {
            return results;
        }
    }

}
