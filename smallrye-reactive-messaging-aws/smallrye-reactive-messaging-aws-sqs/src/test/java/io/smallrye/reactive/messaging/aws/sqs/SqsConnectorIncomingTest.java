package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.aws.sqs.base.SqsTestBase;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessage;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsIncomingMessageMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SqsConnectorIncomingTest extends SqsTestBase {

    @Test
    void should_receive_message_string() {
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
        });
    }

    @Test
    void should_receive_message_msg() {
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

            final SqsIncomingMessageMetadata metadata =
                    stringMessage.getMetadata(SqsIncomingMessageMetadata.class).orElse(null);

            assertThat(metadata).isNotNull();
            assertThat(metadata.getMsg()).isNotNull();
            assertThat(metadata.getMsg().messageId()).isNotBlank();
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
}
