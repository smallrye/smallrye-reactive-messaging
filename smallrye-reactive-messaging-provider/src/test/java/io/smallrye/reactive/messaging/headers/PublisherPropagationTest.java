package io.smallrye.reactive.messaging.headers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class PublisherPropagationTest extends WeldTestBaseWithoutTails {

    @Test
    public void test() {
        addBeanClass(SimplePropagationTest.Source.class, SimplePropagationTest.Sink.class);
        addBeanClass(ProcessorReturningPublisherOfPayload.class, ProcessorRetuningPublisherOfMessage.class);
        initialize();
        SimplePropagationTest.Sink sink = container.select(SimplePropagationTest.Sink.class).get();
        await().until(() -> sink.list().size() == 40);
        assertThat(sink.list()).allSatisfy(message -> {
            Headers headers = message.getHeaders();
            assertThat(headers.get("message")).isEqualTo("hello");
            assertThat(headers.getAsInteger("key", -1)).isNotEqualTo(-1);
            assertThat(headers.get("foo")).isNull();
        }).hasSize(40);

    }

    @ApplicationScoped
    public static class ProcessorReturningPublisherOfPayload {
        @Incoming("source")
        @Outgoing("intermediate")
        public Publisher<String> process(String payload) {
            return Flowable.just(payload, payload);
        }

    }

    @ApplicationScoped
    public static class ProcessorRetuningPublisherOfMessage {
        @Incoming("intermediate")
        @Outgoing("sink")
        public Publisher<Message<String>> process(Message<String> input) {
            return Flowable.just(
                    input.withHeaders(input.getHeaders().without("foo").with("message", "hello")),
                    input.withHeaders(input.getHeaders().without("foo").with("message", "hello")));
        }
    }

}
