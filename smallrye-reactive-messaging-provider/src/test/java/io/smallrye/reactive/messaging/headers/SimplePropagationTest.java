package io.smallrye.reactive.messaging.headers;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class SimplePropagationTest extends WeldTestBaseWithoutTails {

    @Test
    public void test() {
        addBeanClass(Source.class, Sink.class, SimplePayloadProcessor.class, SimpleMessageProcessor.class);
        initialize();
        Sink sink = container.select(Sink.class).get();

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
        public String process(String payload) {
            return payload + payload;
        }

    }

    @ApplicationScoped
    public static class SimpleMessageProcessor {
        @Incoming("intermediate")
        @Outgoing("sink")
        public Message<String> process(Message<String> input) {
            return input.withHeaders(input.getHeaders().without("foo").with("message", "hello"));
        }
    }

    @ApplicationScoped
    public static class Source {

        @Outgoing("source")
        public Publisher<Message<String>> source() {
            return Flowable.range(1, 10)
                    .map(i -> Message.of(Integer.toString(i), Headers.of("key", i, "foo", "bar")));
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
