package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class InboundMessageTest extends SqsTestBase {

    @Test
    void testInboundMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        addBeans(Customizer.class);
        int expected = 10;
        String queueUrl = createQueue(queue);
        sendMessage(queueUrl, expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                        .dataType("String").stringValue("value").build())));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.queue", queue);

        IncomingMessageApp app = runApplication(config, IncomingMessageApp.class);
        await().until(() -> app.received().size() == expected);
        assertThat(app.received()).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Identifier("source")
    @ApplicationScoped
    public static class Customizer implements SqsReceiveMessageRequestCustomizer {

        @Override
        public void customize(ReceiveMessageRequest.Builder builder) {
            builder.messageAttributeNames("key");
        }
    }

    @ApplicationScoped
    public static class IncomingMessageApp {

        List<Message> received = new CopyOnWriteArrayList<>();

        @Incoming("source")
        public void process(Message message) {
            received.add(message);
        }

        public List<Message> received() {
            return received;
        }
    }

    @Test
    void testInboundMessageAck() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        addBeans(Customizer.class);
        int expected = 10;
        String queueUrl = createQueue(queue);
        sendMessage(queueUrl, expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                        .dataType("String").stringValue("value").build())));

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.queue", queue);

        IncomingMessageAckApp app = runApplication(config, IncomingMessageAckApp.class);
        await().until(() -> app.received().size() == expected);
        assertThat(app.received()).hasSize(expected)
                .allSatisfy(m -> assertThat(m.getMetadata(SqsIncomingMetadata.class)).isNotEmpty()
                        .map(SqsIncomingMetadata::getMessage).isNotEmpty())
                .extracting(m -> m.getPayload()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @ApplicationScoped
    public static class IncomingMessageAckApp {

        List<org.eclipse.microprofile.reactive.messaging.Message<String>> received = new CopyOnWriteArrayList<>();

        @Incoming("source")
        public CompletionStage<Void> process(org.eclipse.microprofile.reactive.messaging.Message<String> message) {
            received.add(message);
            return message.ack();
        }

        public List<org.eclipse.microprofile.reactive.messaging.Message<String>> received() {
            return received;
        }
    }
}
