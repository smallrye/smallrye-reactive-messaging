package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SimpleAsyncPropagationTest extends WeldTestBaseWithoutTails {

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void test() {
        addBeanClass(SimplePropagationTest.Source.class, SimplePropagationTest.Sink.class);
        addBeanClass(SimplePayloadProcessor.class, SimpleMessageProcessor.class);
        initialize();
        SimplePropagationTest.Sink sink = container.select(SimplePropagationTest.Sink.class).get();
        await().until(() -> sink.list().size() == 10);
        assertThat(sink.list()).allSatisfy(message -> {
            SimplePropagationTest.CounterMetadata c = message.getMetadata(SimplePropagationTest.CounterMetadata.class).get();
            SimplePropagationTest.MsgMetadata m = message.getMetadata(SimplePropagationTest.MsgMetadata.class).get();
            assertThat(m.getMessage()).isEqualTo("hello");
            assertThat(c.getCount()).isNotEqualTo(0);
            assertThat(message.getMetadata()).hasSize(2);
        }).hasSize(10);

    }

    @ApplicationScoped
    public static class SimplePayloadProcessor {
        @Incoming("source")
        @Outgoing("intermediate")
        public CompletionStage<String> process(String payload) {
            return CompletableFuture.supplyAsync(() -> payload + payload);
        }

    }

    @ApplicationScoped
    public static class SimpleMessageProcessor {
        @Incoming("intermediate")
        @Outgoing("sink")
        public CompletionStage<Message<String>> process(Message<String> input) {
            return CompletableFuture.supplyAsync(
                    () -> input.withMetadata(input.getMetadata().with(new SimplePropagationTest.MsgMetadata("hello"))));
        }
    }

}
