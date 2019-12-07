package io.smallrye.reactive.messaging.headers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SimpleAsyncPropagationTest extends WeldTestBaseWithoutTails {

    @Test
    public void test() {
        addBeanClass(SimplePropagationTest.Source.class, SimplePropagationTest.Sink.class);
        addBeanClass(SimplePayloadProcessor.class, SimpleMessageProcessor.class);
        initialize();
        SimplePropagationTest.Sink sink = container.select(SimplePropagationTest.Sink.class).get();
        await().until(() -> sink.list().size() == 10);
        assertThat(sink.list()).allSatisfy(message -> {
            Headers headers = message.getHeaders();
            assertThat(headers.get("message")).isEqualTo("hello");
            assertThat(headers.getAsInteger("key", -1)).isNotEqualTo(-1);
            assertThat(headers.get("foo")).isNull();
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
                    () -> input.withHeaders(input.getHeaders().without("foo").with("message", "hello")));
        }
    }

}
