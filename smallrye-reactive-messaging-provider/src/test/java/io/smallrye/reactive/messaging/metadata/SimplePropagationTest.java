package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SimplePropagationTest extends WeldTestBaseWithoutTails {

    @Test
    public void test() {
        addBeanClass(Source.class, Sink.class, SimplePayloadProcessor.class, SimpleMessageProcessor.class);
        initialize();
        Sink sink = container.select(Sink.class).get();

        assertThat(sink.list()).allSatisfy(message -> {
            CounterMetadata c = message.getMetadata(CounterMetadata.class)
                    .orElseThrow(() -> new AssertionError("Metadata expected"));
            MsgMetadata m = message.getMetadata(MsgMetadata.class).orElseThrow(() -> new AssertionError("Metadata expected"));
            assertThat(m.getMessage()).isEqualTo("hello");
            assertThat(c.getCount()).isNotEqualTo(0);
        }).hasSize(10);

    }

    @ApplicationScoped
    public static class SimplePayloadProcessor {
        @Incoming("source")
        @Outgoing("intermediate")
        public String process(String payload) {
            return payload + payload;
        }

    }

    @ApplicationScoped
    public static class SimpleMessageProcessor {
        @Incoming("intermediate")
        @Outgoing("sink")
        public Message<String> process(Message<String> input) {
            return input.withMetadata(input.getMetadata().without(MsgMetadata.class).with(new MsgMetadata("hello")));
        }
    }

    public static class CounterMetadata {

        private final int count;

        public CounterMetadata(int count) {
            this.count = count;
        }

        int getCount() {
            return count;
        }

    }

    public static class MsgMetadata {

        private final String message;

        public MsgMetadata(String m) {
            this.message = m;
        }

        String getMessage() {
            return message;
        }

    }

    @ApplicationScoped
    public static class Source {

        @Outgoing("source")
        public Publisher<Message<String>> source() {
            return Multi.createFrom().range(1, 11)
                    .map(i -> Message.of(Integer.toString(i), Metadata.of(new CounterMetadata(i), new MsgMetadata("foo"))));
        }

    }

    @ApplicationScoped
    public static class Sink {
        List<Message<String>> list = new ArrayList<>();

        @Incoming("sink")
        public CompletionStage<Void> consume(Message<String> message) {
            list.add(message);
            return message.ack();
        }

        public List<Message<String>> list() {
            return list;
        }
    }

}
