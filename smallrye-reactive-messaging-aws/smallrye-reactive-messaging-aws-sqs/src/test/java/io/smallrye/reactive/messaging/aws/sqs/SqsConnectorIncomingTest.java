package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.base.SqsTestBase;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessageMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SqsConnectorIncomingTest extends SqsTestBase {

    @Test
    void should_receive_message_via_payload() {
        // given
        MapBasedConfig config = getIncomingConfig();

        TestApp app = runApplication(config, TestApp.class);

        sendMessage("test");

        // when
        await().untilAsserted(() -> {
            List<String> received = app.received();

            // then
            assertThat(received).hasSize(1);
            assertThat(received.get(0)).isEqualTo("test");
            verifyNoInvisibleMessages();
        });
    }

    @Test
    void should_receive_message_via_message() {
        // given
        MapBasedConfig config = getIncomingConfig();

        TestAppMsg app = runApplication(config, TestAppMsg.class);

        sendMessage("test");

        // when
        await().untilAsserted(() -> {
            List<Message<String>> received = app.received();

            // then
            assertThat(received).hasSize(1);

            final Message<String> stringMessage = received.get(0);
            assertThat(stringMessage.getPayload()).isEqualTo("test");

            final SqsIncomingMessageMetadata metadata = stringMessage.getMetadata(SqsIncomingMessageMetadata.class)
                    .orElse(null);

            assertThat(metadata).isNotNull();
            assertThat(metadata.getAwsMessage()).isNotNull();
            assertThat(metadata.getAwsMessage().messageId()).isNotBlank();
            verifyNoInvisibleMessages();
        });
    }

    @Test
    void should_ack_message_with_batching() {
        // given
        MapBasedConfig config = getIncomingConfig()
                .with("mp.messaging.incoming.test.delete.batch.enabled", true)
                .with("mp.messaging.incoming.test.delete.batch.max-size", 10)
                .with("mp.messaging.incoming.test.delete.batch.max-delay", 2)
                .with("mp.messaging.incoming.test.visibility-timeout", 10);

        TestApp app = runApplication(config, TestApp.class);

        sendMessage("test");

        // when
        await().untilAsserted(() -> {
            List<String> received = app.received();

            // then
            assertThat(received).hasSize(1);
            assertThat(received.get(0)).isEqualTo("test");

            // test the delayed batching
            verifyInvisibleMessages(1);
        });
        // then
        await().untilAsserted(this::verifyNoInvisibleMessages);
    }

    @Test
    void should_ack_messages_with_batching() {
        // given
        MapBasedConfig config = getIncomingConfig()
                .with("mp.messaging.incoming.test.delete.batch.enabled", true)
                .with("mp.messaging.incoming.test.delete.batch.max-size", 10)
                .with("mp.messaging.incoming.test.delete.batch.max-delay", 2)
                .with("mp.messaging.incoming.test.visibility-timeout", 10);

        TestApp app = runApplication(config, TestApp.class);

        for (int i = 0; i < 10; i++) {
            sendMessage("test");
        }

        // when
        await().untilAsserted(() -> {
            List<String> received = app.received();

            // then
            assertThat(received).hasSize(10);

            // test the delayed batching
            verifyNoInvisibleMessages();
        });
    }

    @Test
    void should_receive_message_via_message_without_ack() {
        // given
        MapBasedConfig config = getIncomingConfig();

        TestAppMsgNoAck app = runApplication(config, TestAppMsgNoAck.class);

        sendMessage("test");

        // when
        await().untilAsserted(() -> {
            List<Message<String>> received = app.received();

            // then
            assertThat(received).hasSize(1);

            final Message<String> stringMessage = received.get(0);
            assertThat(stringMessage.getPayload()).isEqualTo("test");

            final SqsIncomingMessageMetadata metadata = stringMessage.getMetadata(SqsIncomingMessageMetadata.class)
                    .orElse(null);

            assertThat(metadata).isNotNull();
            assertThat(metadata.getAwsMessage()).isNotNull();
            assertThat(metadata.getAwsMessage().messageId()).isNotBlank();
            verifyInvisibleMessages(1);
        });
    }

    @ApplicationScoped
    public static class TestApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming(QUEUE_NAME)
        void consume(String msg) {
            received.add(msg);
        }

        public List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class TestAppMsg {

        List<Message<String>> received = new CopyOnWriteArrayList<>();

        @Incoming(QUEUE_NAME)
        Uni<Void> consume(Message<String> msg) {
            received.add(msg);
            return Uni.createFrom().completionStage(msg.ack());
        }

        public List<Message<String>> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class TestAppMsgNoAck {

        List<Message<String>> received = new CopyOnWriteArrayList<>();

        @Incoming(QUEUE_NAME)
        Uni<Void> consume(Message<String> msg) {
            received.add(msg);
            return Uni.createFrom().voidItem();
        }

        public List<Message<String>> received() {
            return received;
        }
    }
}
