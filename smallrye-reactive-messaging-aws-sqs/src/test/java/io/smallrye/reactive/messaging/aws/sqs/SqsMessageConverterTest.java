package io.smallrye.reactive.messaging.aws.sqs;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;

public class SqsMessageConverterTest extends SqsTestBase {

    @Test
    void testConsumerWithConverter() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue);

        ConsumerApp app = runApplication(config, ConsumerApp.class);
        await().until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class ConsumerApp {
        List<Message> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(Message message) {
            received.add(message);
        }

        public List<Message> received() {
            return received;
        }
    }
}
