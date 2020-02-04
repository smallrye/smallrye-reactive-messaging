package io.smallrye.reactive.messaging.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.Test;

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
        Message<String> message = Message.<String>newBuilder().payload("hello").build();
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.ack()).isCompleted();

        Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.completedFuture(null);
        message = Message.<String>newBuilder().payload("hello").ack(supplier).build();
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.<String>newBuilder().payload("hello").metadata(Metadata.of(new MyMetadata<>("v"))).build();
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("v");
        assertThat(message.ack()).isCompleted();

        message = Message.<String>newBuilder().payload("hello").metadata(Metadata.of(new MyMetadata<>("v"))).ack(supplier).build();
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).hasSize(1);
        Optional<MyMetadata> o = message.getMetadata(MyMetadata.class);
        assertThat(message.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("v");
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.<String>newBuilder().payload("hello").metadata(Metadata.of(new MyMetadata<>("v"))).ack(null).build();
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("v");
        assertThat(message.getAck()).isNotEqualTo(supplier);
        assertThat(message.ack()).isCompleted();
    }

    @Test
    public void testFrom() {
        Message<String> message = Message.<String>newBuilder().payload("hello").build();
        assertThat(message.getPayload()).isEqualTo("hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.ack()).isCompleted();

        Message<String> message2 = Message.<String>newBuilder().of(message).metadata(Metadata.of(new MyMetadata<>("bar"))).build();
        assertThat(message2.getPayload()).isEqualTo("hello");
        assertThat(message2.getMetadata()).hasSize(1);
        assertThat(message2.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("bar");
        assertThat(message2.ack()).isCompleted();
        assertThat(message).isNotEqualTo(message2);

        Message<String> message3 = Message.<String>newBuilder().of(message2).ack(CompletableFuture::new).build();
        assertThat(message3.getPayload()).isEqualTo("hello");
        assertThat(message3.getMetadata()).hasSize(1);
        assertThat(message3.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("bar");
        assertThat(message3.ack()).isNotCompleted();
        assertThat(message3).isNotEqualTo(message2).isNotEqualTo(message);

        Message<List<String>> message4 = Message.<List<String>>newBuilder().of(message3).payload(Collections.singletonList("foo")).build();
        assertThat(message4.getPayload()).containsExactly("foo");
        assertThat(message4.getMetadata()).hasSize(1);
        assertThat(message4.getMetadata(MyMetadata.class).map(m -> m.v)).hasValue("bar");
        assertThat(message4.ack()).isNotCompleted();
        assertThat(message4).isNotEqualTo(message2).isNotEqualTo(message3).isNotEqualTo(message);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAccessThroughMessage() {
        Message<String> message = Message.<String>newBuilder().payload("hello").metadata(Metadata.of(new MyMetadata<>("value"), new CountMetadata(23))).build();

        assertThat(message.getMetadata()).hasSize(2);
        assertThat(message.getMetadata(MyMetadata.class).map(i -> i.v)).hasValue("value");
        assertThat(message.getMetadata(CountMetadata.class).map(i -> i.count)).hasValue(23);
        assertThat(message.getMetadata(String.class)).isEmpty();
        assertThatThrownBy(() -> message.getMetadata(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
