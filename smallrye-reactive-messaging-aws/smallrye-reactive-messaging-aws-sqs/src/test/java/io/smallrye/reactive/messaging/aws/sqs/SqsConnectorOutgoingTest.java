package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.aws.sqs.base.SqsTestBase;
import io.smallrye.reactive.messaging.aws.sqs.message.SqsOutgoingMessageMetadata;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SqsConnectorOutgoingTest extends SqsTestBase {

    @Test
    void should_send_message_without_batching() {
        // given
        MapBasedConfig config = getOutgoingConfig();

        TestApp app = runApplication(config, TestApp.class);

        // when
        app.sendMessage("test").subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem().assertCompleted();

        // then
        var messages = receiveMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).body()).isEqualTo("test");
    }

    @Test
    void should_send_message_with_batching() {
        // given
        MapBasedConfig config = getOutgoingConfig().with("mp.messaging.outgoing.test.send.batch.enabled", true);

        TestApp app = runApplication(config, TestApp.class);

        // when
        app.sendMessage("test")
                .subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem().assertCompleted();

        // then
        var messages = receiveMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).body()).isEqualTo("test");
    }

    @Test
    void should_send_message_with_metadata() {
        // given
        MapBasedConfig config = getOutgoingConfig();

        TestApp app = runApplication(config, TestApp.class);

        // when
        app.sendMessage("test", new SqsOutgoingMessageMetadata()
                .withMessageAugmenter(b -> b.messageAttributes(Map.of(
                        "One", MessageAttributeValue.builder().dataType("String").stringValue("Piece").build()))))
                .subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem().assertCompleted();

        // then
        var messages = receiveMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).body()).isEqualTo("test");
        assertThat(messages.get(0).messageAttributes()).hasSize(1);
        assertThat(messages.get(0).messageAttributes().get("One")).isNotNull();
        assertThat(messages.get(0).messageAttributes().get("One").stringValue()).isEqualTo("Piece");
    }

    @ApplicationScoped
    public static class TestApp {

        @Inject
        @Channel(QUEUE_NAME)
        MutinyEmitter<String> emitter;

        Uni<Void> sendMessage(String msg) {
            return emitter.send(msg);
        }

        Uni<Void> sendMessage(String msg, SqsOutgoingMessageMetadata metadata) {
            if (metadata != null) {
                return emitter.sendMessage(Message.of(msg, Metadata.of(metadata)));
            } else {
                return emitter.send(msg);
            }
        }
    }
}
