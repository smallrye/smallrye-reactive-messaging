package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

public class MessageTest {

    public static class MyMetadata<T> {

        final T v;

        public MyMetadata(T v) {
            this.v = v;
        }
    }

    public static class CountMetadata {

        final int count;

        public CountMetadata(int value) {
            this.count = value;
        }
    }

    @Test
    public void testMessageCreation() {
        Message<String> message = Message.of("hello");
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.ack()).isCompleted();

        Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.completedFuture(null);
        message = Message.of("hello", supplier);
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.of("hello", Metadata.of(new MyMetadata<>("v")));
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("v");
        assertThat(message.ack()).isCompleted();

        message = Message.of("hello", Metadata.of(new MyMetadata<>("v")), supplier);
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("v");
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.of("hello", Metadata.of(new MyMetadata<>("v")), null);
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("v");
        assertThat(message.getAck()).isNotEqualTo(supplier);
        assertThat(message.ack()).isCompleted();
    }

    @Test
    public void testFrom() {
        Message<String> message = Message.of("hello");
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.ack()).isCompleted();

        Message<String> message2 = message.withMetadata(Metadata.of(new MyMetadata<>("bar")));
        assertThat(message2.getPayload()).isEqualTo("hello");
        assertThat(message2.getMetadata()).hasSize(1);
        assertThat(message2.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("bar");
        assertThat(message2.ack()).isCompleted();
        assertThat(message).isNotEqualTo(message2);

        Message<String> message3 = message2.withAck(CompletableFuture::new);
        assertThat(message3.getPayload()).isEqualTo("hello");
        assertThat(message3.getMetadata()).hasSize(1);
        assertThat(message3.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("bar");
        assertThat(message3.ack()).isNotCompleted();
        assertThat(message3).isNotEqualTo(message2).isNotEqualTo(message);

        Message<List<String>> message4 = message3.withPayload(Collections.singletonList("foo"));
        assertThat(message4.getPayload()).containsExactly("foo");
        assertThat(message4.getMetadata()).hasSize(1);
        assertThat(message4.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("bar");
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

    @Test
    public void testAccessThroughMessage() {
        Message<String> message = Message.of("hello", Metadata.of(new MyMetadata<>("value"), new CountMetadata(23)));

        assertThat(message.getMetadata()).hasSize(2);
        assertThat(message.getMetadata(MyMetadata.class).map(i -> i.v)).hasValue("value");
        assertThat(message.getMetadata(CountMetadata.class).map(i -> i.count)).hasValue(23);
        assertThat(message.getMetadata(String.class)).isEmpty();
        assertThatThrownBy(() -> message.getMetadata(null)).isInstanceOf(IllegalArgumentException.class);
    }

    private static class MyMessage implements Message<String> {

        @Override
        public String getPayload() {
            return "doh!";
        }
    }

}
