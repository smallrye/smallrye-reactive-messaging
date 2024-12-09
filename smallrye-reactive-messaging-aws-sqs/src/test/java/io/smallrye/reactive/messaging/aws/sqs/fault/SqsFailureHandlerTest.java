package io.smallrye.reactive.messaging.aws.sqs.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.aws.sqs.SqsClientProvider;
import io.smallrye.reactive.messaging.aws.sqs.SqsConnector;
import io.smallrye.reactive.messaging.aws.sqs.SqsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

public class SqsFailureHandlerTest extends SqsTestBase {

    @Test
    void testIgnoreNack() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.visibility-timeout", 3);

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(app.seen().size()).isGreaterThanOrEqualTo(expected));

        List<Message> messages = receiveMessages(queue, r -> r.visibilityTimeout(1), expected, Duration.ofMinutes(1));
        assertThat(messages).hasSizeGreaterThanOrEqualTo(expected);
    }

    @Test
    void testVisibilityNack() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.failure-strategy", "visibility");

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(app.seen().size()).isGreaterThanOrEqualTo(expected));

        List<Message> messages = receiveMessages(queue, r -> r.visibilityTimeout(1), expected, Duration.ofMinutes(1));
        assertThat(messages).hasSizeGreaterThanOrEqualTo(expected);
    }

    @Test
    void testDeleteNack() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.visibility-timeout", 10)
                .with("mp.messaging.incoming.data.failure-strategy", "delete");

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(app.seen()).hasSize(expected));

        List<Message> messages = receiveMessages(queue, r -> r.visibilityTimeout(1), expected, Duration.ofSeconds(3));
        assertThat(messages).isEmpty();
    }

    @Test
    void testFailStop() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.failure-strategy", "fail");

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().pollDelay(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(app.seen()).hasSize(1));

    }

    @ApplicationScoped
    public static class ConsumerApp {

        List<String> seen = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(String msg) {
            seen.add(msg);
            throw new RuntimeException("boom");
        }

        public List<String> seen() {
            return seen;
        }
    }

}
