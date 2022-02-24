package io.smallrye.reactive.messaging.pulsar;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SampleAppTest extends WeldTestBase {

    @Test
    void testAppUsingPulsarConnector() throws PulsarClientException {
        // Run app
        ConsumingApp consumingApp = runApplication(config(), ConsumingApp.class);
        // Produce messages
        send(client.newProducer(Schema.STRING)
                .producerName("test-producer")
                .topic(topic)
                .create(), 10, String::valueOf);

        // Check for consumed messages in app
        await().until(() -> consumingApp.results.size() == 10);
    }

    MapBasedConfig config() {
        // TODO complete config
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.service.url", serviceUrl)
                .with("mp.messaging.incoming.data.topic", topic)
                ;
    }

    @ApplicationScoped
    public static class ConsumingApp {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<String> message) {
            results.add(message.getPayload());
            return message.ack();
        }

        public List<String> results() {
            return results;
        }
    }
}
