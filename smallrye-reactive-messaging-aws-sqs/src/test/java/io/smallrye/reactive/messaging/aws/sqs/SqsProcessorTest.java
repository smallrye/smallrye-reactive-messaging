package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

class SqsProcessorTest extends SqsTestBase {

    @Test
    void testProcessor() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 10;
        sendMessage(createQueue(queue), expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String").stringValue(Integer.class.getName()).build())));
        String sinkUrl = createQueue(queue + "-sink");
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue + "-sink");

        ProcessorApp app = runApplication(config, ProcessorApp.class);
        await().until(() -> app.processed().size() == expected);

        assertThat(receiveMessages(sinkUrl, expected, Duration.ofSeconds(10)))
                .hasSize(expected)
                .extracting(Message::body)
                .containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    @ApplicationScoped
    public static class ProcessorApp {

        List<Integer> processed = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Outgoing("sink")
        int consume(int msg) {
            processed.add(msg);
            return msg + 1;
        }

        public List<Integer> processed() {
            return processed;
        }
    }

    @Test
    void testProcessorPauseResume() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 1000;
        sendMessage(createQueue(queue), expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String").stringValue(Integer.class.getName()).build())));
        String sinkUrl = createQueue(queue + "-sink");
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue + "-sink");

        ProcessorApp app = runApplication(config, ProcessorApp.class);
        await().atMost(1, TimeUnit.MINUTES).until(() -> app.processed().size() == expected);

        assertThat(receiveMessages(sinkUrl, expected, Duration.ofMinutes(1)))
                .hasSize(expected)
                .extracting(Message::body)
                .containsExactlyElementsOf(
                        IntStream.range(0, expected).map(i -> i + 1).mapToObj(String::valueOf).collect(Collectors.toList()));
    }

    @Test
    void testProcessorPauseResumeMaxNumberOfMessages() {
        SqsClientProvider.client = getSqsClient();
        addBeans(SqsClientProvider.class);
        int expected = 100;
        sendMessage(createQueue(queue), expected, (i, r) -> r.messageBody(String.valueOf(i))
                .messageAttributes(Map.of(SqsConnector.CLASS_NAME_ATTRIBUTE, MessageAttributeValue.builder()
                        .dataType("String").stringValue(Integer.class.getName()).build())));
        String sinkUrl = createQueue(queue + "-sink");
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queue)
                .with("mp.messaging.incoming.data.max-number-of-messages", 3)
                .with("mp.messaging.outgoing.sink.connector", SqsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.queue", queue + "-sink");

        ProcessorApp app = runApplication(config, ProcessorApp.class);
        await().atMost(1, TimeUnit.MINUTES).until(() -> app.processed().size() == expected);

        assertThat(receiveMessages(sinkUrl, expected, Duration.ofMinutes(1)))
                .hasSize(expected)
                .extracting(Message::body)
                .containsExactlyElementsOf(
                        IntStream.range(0, expected).map(i -> i + 1).mapToObj(String::valueOf).collect(Collectors.toList()));
    }

}
