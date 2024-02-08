package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.GenericPayload;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class OutboundMessageTest extends SqsTestBase {

    @Test
    void testOutboundMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        String queueUrl = createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue);

        runApplication(config, RequestBuilderProducingApp.class);
        var received = receiveMessages(queueUrl, r -> r.messageAttributeNames("key"), expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(10)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testOutboundMetadataMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        String queueUrl = createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue);

        runApplication(config, OutgoingMetadataProducingApp.class);
        var received = receiveMessages(queueUrl, r -> r.messageAttributeNames("key"), expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(10)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testMessage() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        String queueSink = queue + "-sink.fifo";
        queue = queue + ".fifo";
        String queueUrl = createQueue(queue, r -> r.attributes(
                Map.of(QueueAttributeName.FIFO_QUEUE, "true",
                        QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup")));

        sendMessage(queueUrl, 10, (i, r) -> r.messageGroupId("group")
                .messageDeduplicationId("m-" + i)
                .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                        .dataType("String").stringValue("value").build()))
                .messageBody(String.valueOf(i)));

        String queueSinkUrl = createQueue(queueSink, r -> r.attributes(Map.of(QueueAttributeName.FIFO_QUEUE, "true",
                QueueAttributeName.DEDUPLICATION_SCOPE, "messageGroup")));
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queueSink);

        runApplication(config, MessageProducingApp.class);
        var received = receiveMessages(queueSinkUrl, r -> r.messageAttributeNames("key"), expected, Duration.ofMinutes(1));
        assertThat(received).hasSizeGreaterThan(10)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(m -> m.body()).contains("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void testOutboundMessageNacking() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        createQueue(queue);
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue);

        MessageAckingApp app = runApplication(config, MessageAckingApp.class);
        await().until(() -> app.nacked().size() == 10);
    }

    @ApplicationScoped
    public static class RequestBuilderProducingApp {
        @Outgoing("sink")
        public Multi<SendMessageRequest.Builder> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> SendMessageRequest.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .messageBody(String.valueOf(i)));
        }

    }

    @ApplicationScoped
    public static class OutgoingMetadataProducingApp {
        @Outgoing("sink")
        public Multi<Message<String>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> Message.of(String.valueOf(i), Metadata.of(SqsOutboundMetadata.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .build())));
        }

    }

    @ApplicationScoped
    public static class MessageProducingApp {
        @Incoming("data")
        @Outgoing("sink")
        public GenericPayload<software.amazon.awssdk.services.sqs.model.Message> process(
                software.amazon.awssdk.services.sqs.model.Message message) {
            return GenericPayload.of(message, Metadata.of(SqsOutboundMetadata.builder()
                    .groupId("group")
                    .deduplicationId(message.messageId())
                    .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                            .dataType("String").stringValue("value").build()))
                    .build()));
        }

    }

    @ApplicationScoped
    public static class MessageAckingApp {

        List<Integer> acked = new CopyOnWriteArrayList<>();
        List<Integer> nacked = new CopyOnWriteArrayList<>();

        @Outgoing("sink")
        Multi<Message<Integer>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> Message.of(i, Metadata.of(SqsOutboundMetadata.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .stringValue("value").build()))
                            .build())).withAck(() -> {
                                acked.add(i);
                                return CompletableFuture.completedFuture(null);
                            }).withNack(throwable -> {
                                nacked.add(i);
                                return CompletableFuture.completedFuture(null);
                            }));
        }

        public List<Integer> acked() {
            return acked;
        }

        public List<Integer> nacked() {
            return nacked;
        }
    }
}
