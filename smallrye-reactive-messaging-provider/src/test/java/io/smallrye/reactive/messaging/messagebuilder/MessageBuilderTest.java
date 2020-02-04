package io.smallrye.reactive.messaging.messagebuilder;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageBuilderTest {

    @Test
    public void testMessageCreation() {
        Message<String> message = Message.<String>newBuilder().payload("Hello").build();
        assertThat(message.getPayload()).isEqualTo("Hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isNull();
        assertThat(message.ack()).isCompleted();

        Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.completedFuture(null);
        message = Message.<String>newBuilder().payload("Hello").ack(supplier).build();
        assertThat(message.getPayload()).isEqualTo("Hello");
        assertThat(message.getMetadata()).isEmpty();
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.<String>newBuilder().payload("Hello").metadata(Arrays.asList("meta")).ack(supplier).build();
        assertThat(message.getPayload()).isEqualTo("Hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata().iterator().next()).isEqualTo("meta");
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.<String>newBuilder().payload("Hello").metadata(Metadata.of("meta")).ack(supplier).build();
        assertThat(message.getPayload()).isEqualTo("Hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata().iterator().next()).isEqualTo("meta");
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();

        message = Message.<String>newBuilder().payload("Hello").addMetadata("meta").ack(supplier).build();
        assertThat(message.getPayload()).isEqualTo("Hello");
        assertThat(message.getMetadata()).hasSize(1);
        assertThat(message.getMetadata().iterator().next()).isEqualTo("meta");
        assertThat(message.getAck()).isEqualTo(supplier);
        assertThat(message.ack()).isCompleted();
    }
}
