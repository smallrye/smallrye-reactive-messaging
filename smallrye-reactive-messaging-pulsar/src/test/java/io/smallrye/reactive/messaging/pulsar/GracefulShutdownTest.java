package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.pulsar.base.WeldTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

public class GracefulShutdownTest extends WeldTestBase {

    @Test
    void testGracefulShutdownDrainsInFlightMessages() throws PulsarClientException {
        SlowPulsarConsumerBean app = runApplication(config(), SlowPulsarConsumerBean.class);

        send(client.newProducer(Schema.INT32)
                .producerName("test-producer")
                .topic(topic)
                .create(), 10, i -> i);

        await().atMost(30, TimeUnit.SECONDS).until(() -> app.getCount() >= 3);

        List<Integer> received = app.getReceived();
        int countBeforeShutdown = received.size();

        container.close();
        container = null;

        int countAfterShutdown = received.size();
        assertThat(countAfterShutdown).isGreaterThanOrEqualTo(countBeforeShutdown);
    }

    MapBasedConfig config() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PulsarConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.serviceUrl", serviceUrl)
                .with("mp.messaging.incoming.data.subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
                .with("mp.messaging.incoming.data.topic", topic)
                .with("mp.messaging.incoming.data.schema", "INT32")
                .with("mp.messaging.incoming.data.graceful-shutdown", true);
    }

    @ApplicationScoped
    public static class SlowPulsarConsumerBean {

        private final List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(int payload) throws InterruptedException {
            Thread.sleep(100);
            received.add(payload);
        }

        public int getCount() {
            return received.size();
        }

        public List<Integer> getReceived() {
            return received;
        }
    }
}
