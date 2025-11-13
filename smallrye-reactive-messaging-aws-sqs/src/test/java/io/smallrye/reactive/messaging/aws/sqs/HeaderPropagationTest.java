package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class HeaderPropagationTest extends SqsTestBase {

    @Test
    public void testFromAppToSqs() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        String queueUrl = createQueue(queue);
        runApplication(new MapBasedConfig()
                .with("mp.messaging.outgoing.sqs.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sqs.queue", queue)
                .with("mp.messaging.outgoing.sqs.tracing-enabled", true), MyAppGeneratingData.class);

        var messages = receiveMessages(queueUrl, r -> r.messageAttributeNames("prop"), 10, Duration.ofSeconds(10));

        assertThat(messages).hasSize(10).allSatisfy(entry -> {
            assertThat(entry.body()).isNotNull();
            assertThat(entry.messageAttributes().get("prop").stringValue()).isEqualTo("bar");
        });
    }

    @Test
    public void testFromSqsToAppToSqs() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        String queueUrl = createQueue(queue);
        String resultQueueUrl = createQueue(queue + "-result");
        runApplication(new MapBasedConfig()
                .with("mp.messaging.incoming.source.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.source.queue", queue)
                .with("mp.messaging.incoming.source.tracing-enabled", true)
                .with("mp.messaging.outgoing.sqs.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sqs.queue", queue + "-result")
                .with("mp.messaging.outgoing.sqs.tracing-enabled", true), MyAppProcessingData.class);

        sendMessage(queueUrl, 10, (i, r) -> {
            Map<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("_classname", MessageAttributeValue.builder()
                    .stringValue("java.lang.Integer").dataType("String").build());
            r.messageAttributes(attributes)
                    .messageBody(Integer.toString(i));
        });

        var messages = receiveMessages(resultQueueUrl, r -> r.messageAttributeNames("prop"), 10, Duration.ofSeconds(10));

        assertThat(messages).hasSize(10).allSatisfy(entry -> {
            assertThat(entry.body()).isNotNull();
            assertThat(entry.messageAttributes().get("prop").stringValue()).isEqualTo("bar");
        });
    }

    @ApplicationScoped
    public static class MyAppGeneratingData {

        @Outgoing("source")
        public Multi<Integer> source() {
            return Multi.createFrom().range(0, 10);
        }

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            HashMap<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("prop", MessageAttributeValue.builder()
                    .stringValue("bar").dataType("String").build());
            return Message.of(input.getPayload())
                    .withMetadata(Metadata.of(SqsOutboundMetadata.builder()
                            .messageAttributes(attributes)
                            .build()));
        }

        @Incoming("p1")
        @Outgoing("sqs")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

    @ApplicationScoped
    public static class MyAppProcessingData {

        @Incoming("source")
        @Outgoing("p1")
        public Message<Integer> processMessage(Message<Integer> input) {
            HashMap<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("prop", MessageAttributeValue.builder()
                    .stringValue("bar").dataType("String").build());
            return Message.of(input.getPayload())
                    .withMetadata(Metadata.of(SqsOutboundMetadata.builder()
                            .messageAttributes(attributes)
                            .build()));
        }

        @Incoming("p1")
        @Outgoing("sqs")
        public String processPayload(int payload) {
            return Integer.toString(payload);
        }
    }

}
