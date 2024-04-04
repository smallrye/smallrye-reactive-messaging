package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

public class SqsClientBeanTest extends SqsTestBase {

    @Test
    void testProvidedSqsClient() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        var queue = "test-queue-for-consuming-data";
        var sink = "test-queue-for-sink-data";
        int expected = 10;
        String queueUrl = createQueue(queue);
        String sinkUrl = createQueue(sink);
        sendMessage(queueUrl, expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", sink);

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().until(() -> app.received().size() == expected);
        assertThat(receiveMessages(sinkUrl, 10, Duration.ofSeconds(10))).hasSize(10);
    }

    @ApplicationScoped
    public static class ConsumerApp {
        List<Message> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Outgoing("sink")
        public Message consume(Message message) {
            received.add(message);
            return message;
        }

        public List<Message> received() {
            return received;
        }
    }
}
