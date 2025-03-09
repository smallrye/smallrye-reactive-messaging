package io.smallrye.reactive.messaging.aws.sns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sqs.model.Message;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OutboundMessageTest extends SnsTestBase {

    @Test
    void should_send_message_with_attributes_via_builder() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

        // when
        runApplication(config, RequestBuilderProducingApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> r.messageAttributeNames("key"), expected,
                Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .allSatisfy(m -> assertThat(m.messageAttributes()).containsKey("key"))
                .allSatisfy(m -> assertThat(m.messageAttributes().get("key").stringValue()).isEqualTo("value"))
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
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

    @Test
    void should_provide_message_id_from_response() {
        // given
        // currently the OutgoingInterceptorDecorator overwrites the OutgoingMessageMetadata (omd)
        // this results in the issue that omd cannot be accessed except in the interceptor.
        var config = initClientViaProvider(topic, false);
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 1;

        // when
        var app = runApplication(config, EmitterApp.class);
        final String messageId = app.sendMessage("test").await().atMost(Duration.ofSeconds(10));

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .extracting(Message::body).containsExactlyInAnyOrder("test");
        assertThat(messageId).isNotNull();
    }

    @ApplicationScoped
    public static class EmitterApp {

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

    @Test
    void should_send_message_with_attributes_via_metadata_message() {
        // given
        var config = initClientViaProvider();
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 10;

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

    @ApplicationScoped
    public static class OutgoingMetadataProducingApp {
        @Outgoing("sink")
        public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> produce() {
            return Multi.createFrom().range(0, 10)
                    .map(i -> org.eclipse.microprofile.reactive.messaging.Message
                            .of(String.valueOf(i), Metadata.of(SnsOutboundMetadata.builder()
                                    .messageAttributes(Map.of(
                                            "key", MessageAttributeValue.builder()
                                                    .dataType("String").stringValue("value").build()))
                                    .build())));
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

    @Test
    void should_nack_messages() {
        // given
        var config = initClientViaProvider(topic);
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

    @Test
    void should_send_some_messages_without_killing_the_stream() {
        // given
        var config = initClientViaProvider(topic);
        var subscription = createTopicWithQueueSubscription(topic);
        int expected = 9;

        // when
        runApplication(config, SomeFailureApp.class);

        // then
        var received = receiveAndDeleteMessages(subscription.queueUrl(), r -> {
        }, expected, Duration.ofSeconds(10));
        assertThat(received).hasSize(expected)
                .extracting(Message::body).containsExactlyInAnyOrder("0", "1", "2", "3", "4", "6", "7", "8", "9");
    }

    @ApplicationScoped
    public static class SomeFailureApp {
        @Outgoing("sink")
        public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> produce() {
            return Multi.createFrom().range(0, 10).map(i -> {
                var builder = SnsOutboundMetadata.builder();

                if (i == 5) {
                    builder.topicArn("arn:aws:sns:us-east-1:000000000000:unknown-topic");
                }

                var metadata = builder.build();
                return org.eclipse.microprofile.reactive.messaging.Message.of(String.valueOf(i), Metadata.of(metadata));
            });
        }
    }

    private MapBasedConfig initClientViaProvider() {
        return initClientViaProvider(topic);
    }

    private MapBasedConfig initClientViaProvider(String name) {
        return initClientViaProvider(name, true);
    }

    private MapBasedConfig initClientViaProvider(String name, boolean withInterceptor) {
        SnsTestClientProvider.client = getSnsClient();
        if (withInterceptor) {
            addBeans(SnsTestClientProvider.class, LogFailureInterceptor.class);
        } else {
            addBeans(SnsTestClientProvider.class);
        }
        return new MapBasedConfig()
                .with("mp.messaging.outgoing.sink.connector", SnsConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.sink.topic", name)
                .with("mp.messaging.outgoing.sink.topic.arn", guessTopicName(name));
    }
}
