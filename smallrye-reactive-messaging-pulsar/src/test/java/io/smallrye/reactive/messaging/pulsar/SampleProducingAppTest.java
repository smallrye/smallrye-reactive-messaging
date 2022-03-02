package io.smallrye.reactive.messaging.pulsar;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class SampleProducingAppTest extends WeldTestBase {

    private static final int NUMBER_OF_MESSAGES = 100;

    @Test
    void testAppUsingPulsarConnector() throws PulsarClientException {
        // Run app
        ProducingApp producingApp = runApplication(config(), ProducingApp.class);

        // create consumer
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-" + topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName("test-consumer")
                .subscribe();

        // gather consumes messages
        List<String> received = new CopyOnWriteArrayList<>();
        receive(consumer, NUMBER_OF_MESSAGES, message -> {
            try {
                received.add(message.getValue());
                consumer.acknowledge(message);
            } catch (Exception e) {
                consumer.negativeAcknowledge(message);
            }
        });

        // wait until we have gathered all the expected messages
        await().until(() -> received.size() >= NUMBER_OF_MESSAGES);
    }

    MapBasedConfig config() {
        // TODO complete config
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.data.service-url", serviceUrl)
                .with("mp.messaging.outgoing.data.topic", topic);
    }

    @ApplicationScoped
    public static class ProducingApp {

        @Produces
        @Identifier("data")
        static Schema<String> schema = Schema.STRING;

        @Outgoing("data")
        public Multi<String> produce() {
            return Multi.createFrom().range(0, NUMBER_OF_MESSAGES).map(i -> "" + i);
        }
    }
}
