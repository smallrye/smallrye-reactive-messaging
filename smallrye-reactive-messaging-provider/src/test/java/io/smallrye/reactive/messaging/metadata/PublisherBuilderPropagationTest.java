package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class PublisherBuilderPropagationTest extends WeldTestBaseWithoutTails {

    @Test
    public void test() {
        addBeanClass(SimplePropagationTest.Source.class, SimplePropagationTest.Sink.class);
        addBeanClass(ProcessorReturningPublisherOfPayload.class, ProcessorRetuningPublisherOfMessage.class);
        initialize();
        SimplePropagationTest.Sink sink = container.select(SimplePropagationTest.Sink.class).get();
        await().until(() -> sink.list().size() == 40);
        assertThat(sink.list()).allSatisfy(message -> {
            Metadata metadata = message.getMetadata();
            assertThat((String) metadata.get("message")).isEqualTo("hello");
            assertThat(metadata.getAsInteger("key", -1)).isNotEqualTo(-1);
            assertThat((String) metadata.get("foo")).isNull();
        }).hasSize(40);

    }

    @ApplicationScoped
    public static class ProcessorReturningPublisherOfPayload {
        @Incoming("source")
        @Outgoing("intermediate")
        public PublisherBuilder<String> process(String payload) {
            return ReactiveStreams.of(payload, payload);
        }

    }

    @ApplicationScoped
    public static class ProcessorRetuningPublisherOfMessage {
        @SuppressWarnings("unchecked")
        @Incoming("intermediate")
        @Outgoing("sink")
        public PublisherBuilder<Message<String>> process(Message<String> input) {
            return ReactiveStreams.of(
                    input.withMetadata(input.getMetadata().without("foo").with("message", "hello")),
                    input.withMetadata(input.getMetadata().without("foo").with("message", "hello")));
        }
    }

}
