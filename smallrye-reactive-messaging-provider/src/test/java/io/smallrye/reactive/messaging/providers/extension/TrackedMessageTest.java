package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

class TrackedMessageTest {

    private final AtomicInteger processed = new AtomicInteger();

    @Test
    void keepsPayloadAndMetadata() {
        Metadata metadata = Metadata.of(new Object());
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", metadata), processed::incrementAndGet);

        assertThat(tracked.getPayload()).isEqualTo("a");
        assertThat(tracked.getMetadata()).isSameAs(metadata);
        assertThat(processed).hasValue(0);
    }

    @Test
    void describesTheOriginalMessage() {
        Message<String> original = Message.of("a");

        assertThat(new TrackedMessage<>(original, processed::incrementAndGet)).hasToString(original.toString());
    }

    @Test
    void synchronousAckReturnsTheDelegateStage() {
        CompletableFuture<Void> done = CompletableFuture.completedFuture(null);
        AtomicReference<Metadata> ackMetadata = new AtomicReference<>();
        Metadata metadata = Metadata.of("x");
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", Metadata.empty(), m -> {
            ackMetadata.set(m);
            return done;
        }), processed::incrementAndGet);

        assertThat(tracked.ack(metadata)).isSameAs(done);
        assertThat(ackMetadata).hasValue(metadata);
        assertThat(processed).hasValue(1);
        assertThat(tracked.isProcessed()).isTrue();
    }

    @Test
    void asynchronousAckIsProcessedWhenItCompletes() {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", () -> pending), processed::incrementAndGet);

        CompletionStage<Void> result = tracked.ack();
        assertThat(processed).hasValue(0);
        assertThat(result.toCompletableFuture()).isNotDone();

        pending.complete(null);
        assertThat(processed).hasValue(1);
        assertThat(result.toCompletableFuture()).isCompleted();
    }

    @Test
    void failedAckIsProcessed() {
        CompletableFuture<Void> pending = new CompletableFuture<>();
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", () -> pending), processed::incrementAndGet);

        CompletionStage<Void> result = tracked.ack();
        pending.completeExceptionally(new Exception("boom"));
        assertThat(processed).hasValue(1);
        assertThat(result.toCompletableFuture()).isCompletedExceptionally();
    }

    @Test
    void ackThrowingIsProcessed() {
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", (Supplier<CompletionStage<Void>>) () -> {
            throw new IllegalStateException("boom");
        }), processed::incrementAndGet);

        assertThatThrownBy(tracked::ack).isInstanceOf(IllegalStateException.class);
        assertThat(processed).hasValue(1);
    }

    @Test
    void nackIsProcessedWithTheReason() {
        AtomicReference<Throwable> nackReason = new AtomicReference<>();
        Exception reason = new Exception("boom");
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", Metadata.empty(),
                m -> CompletableFuture.completedFuture(null), (t, m) -> {
                    nackReason.set(t);
                    return CompletableFuture.completedFuture(null);
                }), processed::incrementAndGet);

        tracked.nack(reason).toCompletableFuture().join();
        assertThat(nackReason).hasValue(reason);
        assertThat(processed).hasValue(1);
    }

    @Test
    void nackWithoutReasonIsRejectedAndNotProcessed() {
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a"), processed::incrementAndGet);

        assertThatThrownBy(() -> tracked.nack(null)).isInstanceOf(IllegalArgumentException.class);
        assertThat(processed).hasValue(0);
    }

    @Test
    void isProcessedOnceWhenAckedAndNackedSeveralTimes() {
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a"), processed::incrementAndGet);

        tracked.ack().toCompletableFuture().join();
        tracked.ack().toCompletableFuture().join();
        tracked.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(processed).hasValue(1);
    }

    @Test
    void copiesAcknowledgeThroughTheTrackedMessage() {
        AtomicInteger acks = new AtomicInteger();
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", () -> {
            acks.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }), processed::incrementAndGet);

        Message<Integer> copy = tracked.withPayload(1).addMetadata("x");
        assertThat(processed).hasValue(0);

        copy.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(processed).hasValue(1);
    }

    @Test
    void copiesNackThroughTheTrackedMessage() {
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a"), processed::incrementAndGet);

        tracked.withPayload(1).nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(processed).hasValue(1);
    }

    @Test
    void nullAckStageIsProcessed() {
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a", (Supplier<CompletionStage<Void>>) () -> null),
                processed::incrementAndGet);

        assertThat(tracked.ack().toCompletableFuture()).isCompleted();
        assertThat(processed).hasValue(1);
    }

    @Test
    void unwrapReturnsTheTrackedMessageForItsOwnTypes() {
        TrackedMessage<String> tracked = new TrackedMessage<>(new ConnectorMessage(), processed::incrementAndGet);

        assertThat(tracked.unwrap(Message.class)).isSameAs(tracked);
        assertThat(tracked.unwrap(TrackedMessage.class)).isSameAs(tracked);
    }

    @Test
    void unwrapReturnsTheOriginalMessage() {
        ConnectorMessage original = new ConnectorMessage();
        TrackedMessage<String> tracked = new TrackedMessage<>(original, processed::incrementAndGet);

        assertThat(tracked.unwrap(ConnectorMessage.class)).isSameAs(original);
        assertThat(processed).hasValue(0);
    }

    @Test
    void unwrapDelegatesToTheOriginalMessage() {
        TrackedMessage<String> tracked = new TrackedMessage<>(new ConnectorMessage(), processed::incrementAndGet);

        assertThat(tracked.unwrap(StringBuilder.class)).hasToString("native");
    }

    @Test
    void unwrapToAnUnsupportedTypeIsRejected() {
        TrackedMessage<String> tracked = new TrackedMessage<>(new ConnectorMessage(), processed::incrementAndGet);

        assertThatThrownBy(() -> tracked.unwrap(Integer.class)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> tracked.unwrap(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ackingTheUnwrappedMessageBypassesTheTracking() {
        ConnectorMessage original = new ConnectorMessage();
        TrackedMessage<String> tracked = new TrackedMessage<>(original, processed::incrementAndGet);

        // The documented caveat: the message stays in flight.
        tracked.unwrap(ConnectorMessage.class).ack().toCompletableFuture().join();
        assertThat(original.acks).hasValue(1);
        assertThat(processed).hasValue(0);
        assertThat(tracked.isProcessed()).isFalse();

        tracked.ack().toCompletableFuture().join();
        assertThat(original.acks).hasValue(2);
        assertThat(processed).hasValue(1);
    }

    /**
     * A connector message type, unwrapping to a native type.
     */
    static class ConnectorMessage implements Message<String> {

        final AtomicInteger acks = new AtomicInteger();

        @Override
        public String getPayload() {
            return "a";
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            acks.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
            return this::ack;
        }

        @Override
        public <C> C unwrap(Class<C> unwrapType) {
            if (unwrapType == StringBuilder.class) {
                return unwrapType.cast(new StringBuilder("native"));
            }
            return Message.super.unwrap(unwrapType);
        }
    }

    @Test
    void injectedMetadataReachesTheTrackedAndTheOriginalMessage() {
        InjectableMessage original = new InjectableMessage();
        TrackedMessage<String> tracked = new TrackedMessage<>(original, processed::incrementAndGet);

        tracked.injectMetadata(new StringBuilder("transaction"));

        assertThat(tracked.getMetadata(StringBuilder.class)).isPresent();
        assertThat(original.getMetadata(StringBuilder.class)).isPresent();
    }

    @Test
    void injectedMetadataStaysOnTheTrackedMessageWhenTheOriginalIsNotInjectable() {
        TrackedMessage<String> tracked = new TrackedMessage<>(Message.of("a"), processed::incrementAndGet);

        tracked.injectMetadata(new StringBuilder("transaction"));

        assertThat(tracked.getMetadata(StringBuilder.class)).isPresent();
    }

    @Test
    void payloadAndMetadataAreReadFromTheOriginalMessage() {
        MutableMessage original = new MutableMessage();
        TrackedMessage<String> tracked = new TrackedMessage<>(original, processed::incrementAndGet);

        original.payload = "b";
        original.metadata = Metadata.of(new StringBuilder("late"));

        assertThat(tracked.getPayload()).isEqualTo("b");
        assertThat(tracked.getMetadata(StringBuilder.class)).isPresent();
    }

    @Test
    void metadataInjectedIntoTheTrackedMessageComesOnTopOfTheOriginalMetadata() {
        MutableMessage original = new MutableMessage();
        original.metadata = Metadata.of("from-original");
        TrackedMessage<String> tracked = new TrackedMessage<>(original, processed::incrementAndGet);

        tracked.injectMetadata(new StringBuilder("injected"));

        assertThat(tracked.getMetadata(String.class)).hasValue("from-original");
        assertThat(tracked.getMetadata(StringBuilder.class)).isPresent();
        // Not injectable: the original is unchanged.
        assertThat(original.getMetadata(StringBuilder.class)).isEmpty();
    }

    static class MutableMessage implements Message<String> {

        volatile String payload = "a";
        volatile Metadata metadata = Metadata.empty();

        @Override
        public String getPayload() {
            return payload;
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }
    }

    static class InjectableMessage implements io.smallrye.reactive.messaging.providers.MetadataInjectableMessage<String> {

        private Metadata metadata = Metadata.empty();

        @Override
        public String getPayload() {
            return "a";
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public void injectMetadata(Object metadataObject) {
            metadata = metadata.with(metadataObject);
        }
    }
}
