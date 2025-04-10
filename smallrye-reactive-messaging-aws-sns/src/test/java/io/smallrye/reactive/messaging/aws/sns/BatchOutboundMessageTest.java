package io.smallrye.reactive.messaging.aws.sns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.PublishBatchResultEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sqs.model.Message;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class BatchOutboundMessageTest extends SnsTestBase {

    @Test
    void should_send_batch_messages_via_builder() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        var expected = 10;

        // when
        runApplication(config, RequestBuilderProducingApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> r.messageAttributeNames("key"), expected,
                Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void should_send_batch_messages_via_builder_with_batch_size() {
        // given
        var config = initClientViaProvider()
                .with("mp.messaging.outgoing.sink.batch-size", 3)
                .with("mp.messaging.outgoing.sink.batch-delay", 1000);
        var subscription = createTopicWithQueueSubscription(topic);
        var expected = 10;

        // when
        runApplication(config, RequestBuilderProducingApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> r.messageAttributeNames("key"), expected,
                Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void should_send_batch_messages_via_builder_with_batch_delay() {
        // given
        var config = initClientViaProvider()
                .with("mp.messaging.outgoing.sink.batch-size", 10)
                .with("mp.messaging.outgoing.sink.batch-delay", 1000);
        var subscription = createTopicWithQueueSubscription(topic);
        var expected = 3;

        // when
        runApplication(config, IncompleteBatchApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> r.messageAttributeNames("key"), expected,
                Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2");
    }

    @Test
    void should_send_batch_messages_via_metadata() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        var expected = 10;

        // when
        runApplication(config, OutgoingMetadataProducingApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> r.messageAttributeNames("key"), expected,
                Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void should_nack_messages() {
        // given
        var config = initClientViaProvider();
        createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        InvalidMessageApp app = runApplication(config, InvalidMessageApp.class);

        // then
        await().until(() -> {
            System.out.printf("ack=%d and nack=%d %n", app.acked().size(), app.nacked().size());
            return app.nacked().size() == expected;
        });
    }

    @Test
    void should_send_messages_in_order() {
        // given
        var config = initClientViaProvider(topic + ".fifo");
        var subscription = createTopicWithFifoQueueSubscription(topic);
        int expected = 10;

        // when
        runApplication(config, OutgoingFifoMetadataProducingApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> r.messageAttributeNames("key"), expected,
                Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .extracting(Message::body).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
    }

    @Test
    void should_send_some_messages_without_killing_the_stream() {
        // given
        var config = initClientViaProvider(topic + ".fifo")
                .with("mp.messaging.outgoing.sink.batch-size", 3)
                .with("mp.messaging.outgoing.sink.batch-delay", 1000)
                .with("mp.messaging.outgoing.sink.message-structure", "json");
        var subscription = createTopicWithFifoQueueSubscription(topic);
        // I hoped I could simulate a situation in which only one message in a batch is failing
        // But whatever I tried localStack always answered with 400 on the complete batch.
        // Therefore, all messages of the batch are not delivered.
        // This results in only 7 messages delivered.
        int expected = 7;

        // when
        runApplication(config, SomeFailureApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> {
        }, expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2", "6", "7", "8", "9");
    }

    @Test
    void should_not_reject_batch_if_one_message_fails() {
        // given
        var config = initClientViaProvider(topic)
                .with("mp.messaging.outgoing.sink.batch-size", 3)
                .with("mp.messaging.outgoing.sink.batch-delay", 1000);

        // I hoped I could simulate a situation in which only one message in a batch is failing
        // But whatever I tried localStack always answered with 400 on the complete batch.
        // So instead in this test we check that ack and nack are called properly.
        SnsTestClientProvider.client = spy(SnsTestClientProvider.client);
        doReturn(CompletableFuture.completedFuture(PublishBatchResponse.builder()
                .failed(List.of(
                        BatchResultErrorEntry.builder().code("400").message("5").senderFault(true).id("2").build()))
                .successful(List.of(
                        PublishBatchResultEntry.builder().id("0").build(), // 3
                        PublishBatchResultEntry.builder().id("1").build() // 4
                ))
                .build()))
                .when(SnsTestClientProvider.client)
                .publishBatch(argThat((ArgumentMatcher<PublishBatchRequest>) b -> b
                        .publishBatchRequestEntries().stream()
                        .anyMatch(e -> e.message().equals("5"))));

        createTopicWithQueueSubscription(topic);

        // when
        var app = runApplication(config, PositiveAckNackApp.class);

        // then
        await().untilAsserted(() -> {
            assertThat(app.acked()).hasSize(9);
            assertThat(app.nacked()).hasSize(1);
        });
    }

    @Test
    void should_provide_message_id_from_response() {
        // given
        var config = initClientViaProvider(topic);
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        var app = runApplication(config, EmitterMessageIdApp.class);

        var unis = IntStream.range(0, expected).mapToObj(i -> app.sendMessage(String.valueOf(i)))
                .collect(Collectors.toList());

        var results = Uni.combine().all().unis(unis).with(Function.identity()).await().atMost(Duration.ofSeconds(5));

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .extracting(Message::body).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        assertThat(results).hasSize(expected).doesNotContainNull();
    }

    @ApplicationScoped
    public static class RequestBuilderProducingApp {
        @Outgoing("sink")
        public Multi<PublishRequest.Builder> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> PublishRequest.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .message(String.valueOf(i)));
        }
    }

    @ApplicationScoped
    public static class IncompleteBatchApp {
        @Outgoing("sink")
        public Multi<PublishRequest.Builder> produce() {
            return Multi.createFrom().range(0, 3)
                    .map(i -> PublishRequest.builder()
                            .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                    .dataType("String").stringValue("value").build()))
                            .message(String.valueOf(i)));
        }
    }

    @ApplicationScoped
    public static class OutgoingMetadataProducingApp {
        @Outgoing("sink")
        public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> org.eclipse.microprofile.reactive.messaging.Message.of(String.valueOf(i),
                            Metadata.of(SnsOutboundMetadata.builder()
                                    .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                            .dataType("String").stringValue("value").build()))
                                    .build())));
        }
    }

    @ApplicationScoped
    // No dataType defined in message attribute, which creates invalid messages
    public static class InvalidMessageApp {

        List<Integer> acked = new CopyOnWriteArrayList<>();
        List<Integer> nacked = new CopyOnWriteArrayList<>();

        @Outgoing("sink")
        Multi<org.eclipse.microprofile.reactive.messaging.Message<Integer>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> org.eclipse.microprofile.reactive.messaging.Message.of(i,
                            Metadata.of(SnsOutboundMetadata.builder()
                                    .messageAttributes(Map.of("key", MessageAttributeValue.builder()
                                            .stringValue("value").build()))
                                    .build()))
                            .withAck(() -> {
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

    @ApplicationScoped
    public static class OutgoingFifoMetadataProducingApp {
        @Outgoing("sink")
        public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> org.eclipse.microprofile.reactive.messaging.Message
                            .of(String.valueOf(i), Metadata.of(SnsOutboundMetadata.builder()
                                    .groupId("group")
                                    .deduplicationId(String.valueOf(i))
                                    .messageAttributes(Map.of(
                                            "key", MessageAttributeValue.builder()
                                                    .dataType("String").stringValue("value").build()))
                                    .build())));
        }
    }

    @ApplicationScoped
    public static class SomeFailureApp {
        @Outgoing("sink")
        public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> produce() {
            return Multi.createFrom().range(0, 10).map(i -> {
                var metadata = SnsOutboundMetadata.builder()
                        .groupId("group")
                        .deduplicationId(String.valueOf(i))
                        .build();

                String msg;
                if (i == 5) {
                    msg = "<noJson>5</noJson>";
                } else {
                    msg = "{\"default\":\"" + i + "\",\"email\":\"test\"}";
                }

                return org.eclipse.microprofile.reactive.messaging.Message.of(msg, Metadata.of(metadata));
            });
        }
    }

    @ApplicationScoped
    public static class EmitterMessageIdApp {

        @Inject
        @Channel("sink")
        MutinyEmitter<String> emitter;

        public Uni<String> sendMessage(String payload) {
            var metadata = SnsOutboundMetadata.builder().build();
            OutgoingMessageMetadata<PublishResponse> omd = new OutgoingMessageMetadata<>();
            var msg = org.eclipse.microprofile.reactive.messaging.Message.of(payload, Metadata.of(metadata)).addMetadata(omd);

            return emitter.sendMessage(msg)
                    .onItem().transform(ignore -> omd.getResult().messageId());
        }
    }

    @ApplicationScoped
    public static class PositiveAckNackApp {

        List<Integer> acked = new CopyOnWriteArrayList<>();
        List<Integer> nacked = new CopyOnWriteArrayList<>();

        @Outgoing("sink")
        Multi<org.eclipse.microprofile.reactive.messaging.Message<Integer>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> org.eclipse.microprofile.reactive.messaging.Message.of(i,
                            Metadata.of(SnsOutboundMetadata.builder().build()))
                            .withAck(() -> {
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

    private MapBasedConfig initClientViaProvider() {
        return initClientViaProvider(topic);
    }

    private MapBasedConfig initClientViaProvider(String name) {
        SnsTestClientProvider.client = getSnsClient();
        addBeans(SnsTestClientProvider.class);
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.topic", name)
                .with("mp.messaging.outgoing.sink.topic.arn", guessTopicName(name))
                .with("mp.messaging.outgoing.sink.batch", true);
    }
}
