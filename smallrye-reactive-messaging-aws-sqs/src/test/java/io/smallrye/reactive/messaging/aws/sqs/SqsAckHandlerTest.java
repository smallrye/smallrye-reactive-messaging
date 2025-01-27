package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

public class SqsAckHandlerTest extends SqsTestBase {

    @Test
    void testDeleteAck() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().until(() -> app.received().size() == expected);
        List<Message> messages = receiveMessages(queue, expected, Duration.ofSeconds(3));
        assertThat(messages).isEmpty();
    }

    @Test
    void testIgnoreAck() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.visibility-timeout", 3)
                .with("mp.messaging.incoming.data.ack-strategy", "ignore");

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(app.received().size()).isGreaterThanOrEqualTo(expected));

        List<Message> messages = receiveMessages(queue, r -> r.visibilityTimeout(1), expected, Duration.ofMinutes(1));
        assertThat(messages).hasSizeGreaterThanOrEqualTo(expected);
    }

    @ApplicationScoped
    public static class ConsumerApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }

}
