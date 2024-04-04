package io.smallrye.reactive.messaging.aws.sqs;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class HealthCheckTest extends SqsTestBase {

    @Test
    void testErrorReceivingMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 100;
        sendMessage(createQueue(queue), expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String").stringValue(Integer.class.getName()).build())));
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.max-number-of-messages", 20); // invalid configuration

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().until(() -> !isAlive());
    }

    @ApplicationScoped
    public static class ConsumerApp {

        List<Integer> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(int msg) {
            received.add(msg);
        }

        public List<Integer> received() {
            return received;
        }
    }

    @Test
    void testErrorSendingMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue + "unknown"); // invalid configuration

        ProducerApp app = runApplication(config, ProducerApp.class);
        await().until(() -> !isAlive());
    }

    @ApplicationScoped
    public static class ProducerApp {

        @Outgoing("sink")
        Multi<Integer> produce() {
            return Multi.createFrom().range(0, 10);
        }

    }

}
