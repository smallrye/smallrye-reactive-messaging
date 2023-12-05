package io.smallrye.reactive.messaging.aws.sqs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.aws.sqs.base.SqsTestBase;
import io.smallrye.reactive.messaging.test.common.config.MapBasedConfig;

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
        List<String> messages = receiveMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isEqualTo("test");
    }

    @Test
    void should_send_message_with_batching() {
        // given
        MapBasedConfig config = getOutgoingConfig().with("mp.messaging.outgoing.test.send.batch.enabled", true);

        TestApp app = runApplication(config, TestApp.class);

        // when
        app.sendMessage("test").subscribe().withSubscriber(UniAssertSubscriber.create()).awaitItem().assertCompleted();

        // then
        List<String> messages = receiveMessages();
        assertThat(messages).hasSize(1);
        assertThat(messages.get(0)).isEqualTo("test");
    }

    @ApplicationScoped
    public static class TestApp {

        //List<String> received = new CopyOnWriteArrayList<>();

        @Inject
        @Channel(QUEUE_NAME)
        MutinyEmitter<String> emitter;

        Uni<Void> sendMessage(String msg) {
            return emitter.send(msg);
        }
    }
}
