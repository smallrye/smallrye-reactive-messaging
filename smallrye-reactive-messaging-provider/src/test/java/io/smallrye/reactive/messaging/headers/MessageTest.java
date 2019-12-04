package io.smallrye.reactive.messaging.headers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.Test;

public class MessageTest {

    @Test
    public void testMessageCreation() {
        Message<String> message = Message.of("hello");
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.ack()).isCompleted();

        Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.completedFuture(null);
        message = Message.of("hello", supplier);
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.of("hello", Headers.of("k", "v"));
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getHeaders()).hasSize(1);
        assertThat(message.ack()).isCompleted();

        message = Message.of("hello", Headers.of("k", "v"), supplier);
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getHeaders()).hasSize(1);
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.of("hello", Headers.of("k", "v"), null);
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getHeaders()).hasSize(1);
        assertThat(message.getAck()).isNotEqualTo(supplier);
        assertThat(message.ack()).isCompleted();
    }

    @Test
    public void testFrom() {
        Message<String> message = Message.of("hello");
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getHeaders()).isEmpty();
        assertThat(message.ack()).isCompleted();

        Message<String> message2 = message.withHeaders(Headers.of("foo", "bar"));
        assertThat(message2.getPayload()).isEqualTo("hello");
        assertThat(message2.getHeaders()).hasSize(1);
        assertThat(message2.ack()).isCompleted();
        assertThat(message).isNotEqualTo(message2);

        Message<String> message3 = message2.withAck(CompletableFuture::new);
        assertThat(message3.getPayload()).isEqualTo("hello");
        assertThat(message3.getHeaders()).hasSize(1);
        assertThat(message3.ack()).isNotCompleted();
        assertThat(message3).isNotEqualTo(message2).isNotEqualTo(message);

        Message<List<String>> message4 = message3.withPayload(Collections.singletonList("foo"));
        assertThat(message4.getPayload()).containsExactly("foo");
        assertThat(message4.getHeaders()).hasSize(1);
        assertThat(message4.ack()).isNotCompleted();
        assertThat(message4).isNotEqualTo(message2).isNotEqualTo(message3).isNotEqualTo(message);
    }

    @Test
    public void testUnwrap() {
        Message<String> msg = new MyMessage();

        MyMessage unwrapped = msg.unwrap(MyMessage.class);
        assertThat(unwrapped).isEqualTo(msg);
        assertThat(unwrapped.getPayload()).isEqualTo("doh!");

        assertThatThrownBy(() -> msg.unwrap(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> msg.unwrap(String.class)).isInstanceOf(IllegalArgumentException.class);
    }

    private class MyMessage implements Message<String> {

        @Override
        public String getPayload() {
            return "doh!";
        }
    }

}
