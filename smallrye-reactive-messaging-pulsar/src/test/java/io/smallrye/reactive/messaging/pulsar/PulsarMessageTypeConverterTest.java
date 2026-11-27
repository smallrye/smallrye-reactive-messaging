package io.smallrye.reactive.messaging.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator;
import io.smallrye.reactive.messaging.pulsar.transactions.PulsarTransactionMetadata;

class PulsarMessageTypeConverterTest {

    private final PulsarMessageTypeConverter converter = new PulsarMessageTypeConverter();

    /** Tracks the in-flight messages like a channel with graceful shutdown enabled. */
    private final PausableChannelDecorator.PauserChannel channel = new PausableChannelDecorator.PauserChannel(
            new DemandPauser(), 1000, null, true);

    private final AtomicInteger acks = new AtomicInteger();
    private final AtomicInteger nacks = new AtomicInteger();
    /** The transaction metadata seen by the ack handler, as the Pulsar ack handlers read it. */
    private final List<Object> ackedInTransaction = new CopyOnWriteArrayList<>();

    private final PulsarAckHandler ackHandler = message -> {
        acks.incrementAndGet();
        message.getMetadata(PulsarTransactionMetadata.class).ifPresent(ackedInTransaction::add);
        return Uni.createFrom().voidItem();
    };

    private final PulsarFailureHandler nackHandler = (message, reason, metadata) -> {
        nacks.incrementAndGet();
        return Uni.createFrom().voidItem();
    };

    private static org.apache.pulsar.client.api.Message<String> pulsarMessage(String key, String value) {
        return new MessageImpl<>("topic", MessageId.latest.toString(), Map.of("p", value), value.getBytes(),
                Schema.STRING, new MessageMetadata().setPartitionKey(key));
    }

    private PulsarIncomingMessage<String> incoming(String key, String value) {
        return new PulsarIncomingMessage<>(pulsarMessage(key, value), ackHandler, nackHandler);
    }

    private PulsarIncomingBatchMessage<String> incomingBatch(String... values) {
        List<org.apache.pulsar.client.api.Message<String>> messages = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            messages.add(pulsarMessage("k-" + i, values[i]));
        }
        return new PulsarIncomingBatchMessage<>(new TestMessages<>(messages), ackHandler, nackHandler);
    }

    @SuppressWarnings("unchecked")
    private <T> Message<T> tracked(Message<T> message) {
        return (Message<T>) channel.trackMessage(message);
    }

    @Test
    void canConvertOnlyPulsarMessageTargetsOfInterceptedMessages() {
        PulsarIncomingMessage<String> original = incoming("k", "v");
        Message<String> message = tracked(original);

        assertThat(message).isNotInstanceOf(PulsarIncomingMessage.class);
        assertThat(converter.canConvert(message, PulsarMessage.class)).isTrue();
        assertThat(converter.canConvert(message, PulsarIncomingMessage.class)).isTrue();
        assertThat(converter.canConvert(message, PulsarIdMessage.class)).isTrue();
        assertThat(converter.canConvert(message, String.class)).isFalse();
        assertThat(converter.canConvert(message, PulsarBatchMessage.class)).isFalse();
        assertThat(converter.canConvert(Message.of("v"), PulsarMessage.class)).isFalse();
        // Already a Pulsar message: nothing to restore.
        assertThat(converter.canConvert(original, PulsarMessage.class)).isFalse();
        assertThat(converter.canConvert(PulsarMessage.of("v"), PulsarMessage.class)).isFalse();
        Message<?> restored = converter.convert(message, PulsarMessage.class);
        assertThat(converter.canConvert(restored, PulsarMessage.class)).isFalse();
        assertThat(converter.canConvert(restored, PulsarIncomingMessage.class)).isFalse();
    }

    @Test
    void restoresAMessageAndDelegatesAcknowledgement() {
        PulsarIncomingMessage<String> original = incoming("key", "value");
        Message<String> message = tracked(original);
        assertThat(channel.getInFlightCount()).isEqualTo(1);

        Message<?> converted = converter.convert(message, PulsarIncomingMessage.class);

        assertThat(converted).isInstanceOf(PulsarIncomingMessage.class).isInstanceOf(PulsarIdMessage.class)
                .isInstanceOf(MetadataInjectableMessage.class);
        PulsarIncomingMessage<?> restored = (PulsarIncomingMessage<?>) converted;
        assertThat(restored.getPayload()).isEqualTo("value");
        assertThat(restored.getKey()).isEqualTo("key");
        assertThat(restored.hasKey()).isTrue();
        assertThat(restored.getProperties()).containsEntry("p", "value");
        assertThat(restored.getMessageId()).isEqualTo(original.getMessageId());
        assertThat(restored.unwrap()).isSameAs(original.unwrap());
        assertThat(restored.getMetadata(PulsarIncomingMessageMetadata.class)).isPresent();
        assertThat(restored.unwrap(PulsarIncomingMessage.class)).isSameAs(restored);

        // Acknowledging the restored message acknowledges the tracked message, so it is no longer in flight.
        restored.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(1);
        assertThat(channel.getInFlightCount()).isZero();
        restored.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(1);
    }

    @Test
    void forwardsInjectedMetadataToTheOriginalMessage() {
        PulsarIncomingMessage<String> original = incoming("key", "value");
        Message<String> message = tracked(original);
        PulsarTransactionMetadata txn = new PulsarTransactionMetadata(null);

        // What PulsarTransactions.withTransaction does with the message received by the method.
        PulsarIncomingMessage<?> restored = (PulsarIncomingMessage<?>) converter.convert(message, PulsarMessage.class);
        restored.injectMetadata(txn);

        assertThat(restored.getMetadata(PulsarTransactionMetadata.class)).containsSame(txn);
        assertThat(original.getMetadata(PulsarTransactionMetadata.class)).containsSame(txn);

        // Then the transaction acknowledges the message: the ack handler acknowledges in the transaction.
        restored.ack().toCompletableFuture().join();
        assertThat(ackedInTransaction).containsExactly(txn);
        assertThat(channel.getInFlightCount()).isZero();
    }

    @Test
    void forwardsInjectedMetadataToAnInjectableInterceptedMessage() {
        PulsarIncomingMessage<String> original = incoming("key", "value");
        InjectableInterceptedMessage<String> message = new InjectableInterceptedMessage<>(original);
        PulsarTransactionMetadata txn = new PulsarTransactionMetadata(null);

        PulsarIncomingMessage<?> restored = (PulsarIncomingMessage<?>) converter.convert(message, PulsarMessage.class);
        restored.injectMetadata(txn);

        assertThat(message.getMetadata(PulsarTransactionMetadata.class)).containsSame(txn);
        assertThat(original.getMetadata(PulsarTransactionMetadata.class)).containsSame(txn);
        restored.ack().toCompletableFuture().join();
        assertThat(ackedInTransaction).containsExactly(txn);
    }

    @Test
    void restoresABatchAndDelegatesAcknowledgement() {
        PulsarIncomingBatchMessage<String> original = incomingBatch("a", "b", "c");
        Message<List<String>> message = tracked(original);

        assertThat(converter.canConvert(message, PulsarBatchMessage.class)).isTrue();
        assertThat(converter.canConvert(message, PulsarIncomingBatchMessage.class)).isTrue();
        assertThat(converter.canConvert(message, PulsarMessage.class)).isFalse();
        assertThat(converter.canConvert(original, PulsarBatchMessage.class)).isFalse();

        Message<?> converted = converter.convert(message, PulsarBatchMessage.class);

        assertThat(converted).isInstanceOf(PulsarIncomingBatchMessage.class)
                .isInstanceOf(MetadataInjectableMessage.class);
        @SuppressWarnings("unchecked")
        PulsarIncomingBatchMessage<String> batch = (PulsarIncomingBatchMessage<String>) converted;
        assertThat(converter.canConvert(batch, PulsarBatchMessage.class)).isFalse();
        assertThat(batch.getPayload()).containsExactly("a", "b", "c");
        assertThat(batch.getMessages()).containsExactlyElementsOf(original.getMessages());
        assertThat(batch.getMessages()).extracting(PulsarMessage::getKey).containsExactly("k-0", "k-1", "k-2");
        assertThat(batch.unwrap()).isSameAs(original.unwrap());
        assertThat(batch.getMetadata(PulsarIncomingBatchMessageMetadata.class)).isPresent();

        // Acknowledging the batch delegates to the tracked batch message, which acknowledges every message.
        batch.ack().toCompletableFuture().join();
        assertThat(acks).hasValue(3);
        assertThat(channel.getInFlightCount()).isZero();
        batch.nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(nacks).hasValue(3);
    }

    @Test
    void acknowledgesABatchInTheInjectedTransaction() {
        PulsarIncomingBatchMessage<String> original = incomingBatch("a", "b");
        Message<List<String>> message = tracked(original);
        PulsarTransactionMetadata txn = new PulsarTransactionMetadata(null);

        PulsarIncomingBatchMessage<?> batch = (PulsarIncomingBatchMessage<?>) converter.convert(message,
                PulsarIncomingBatchMessage.class);
        batch.injectMetadata(txn);
        assertThat(original.getMetadata(PulsarTransactionMetadata.class)).containsSame(txn);

        batch.ack().toCompletableFuture().join();
        assertThat(ackedInTransaction).containsExactly(txn, txn);
        assertThat(channel.getInFlightCount()).isZero();
    }

    @Test
    void acknowledgesABatchInTheInjectedTransactionWhenTheOriginalIsNotReachable() {
        PulsarIncomingBatchMessage<String> original = incomingBatch("a", "b");
        // A copy that neither accepts injections nor unwraps to the original.
        Message<List<String>> message = Message.of(original.getPayload(), original.getMetadata(),
                () -> original.ack(), reason -> original.nack(reason));
        PulsarTransactionMetadata txn = new PulsarTransactionMetadata(null);

        PulsarIncomingBatchMessage<?> batch = (PulsarIncomingBatchMessage<?>) converter.convert(message,
                PulsarBatchMessage.class);
        batch.injectMetadata(txn);
        assertThat(original.getMetadata(PulsarTransactionMetadata.class)).isEmpty();

        // The restored batch injects its transaction into the messages of the batch before acknowledging.
        batch.ack().toCompletableFuture().join();
        assertThat(ackedInTransaction).containsExactly(txn, txn);
    }

    /**
     * An intercepted message accepting metadata injections, and forwarding them to the original message.
     */
    private static class InjectableInterceptedMessage<T> implements MetadataInjectableMessage<T> {

        private final Message<T> delegate;
        private volatile Metadata metadata;

        InjectableInterceptedMessage(Message<T> delegate) {
            this.delegate = delegate;
            this.metadata = delegate.getMetadata();
        }

        @Override
        public T getPayload() {
            return delegate.getPayload();
        }

        @Override
        public Metadata getMetadata() {
            return metadata;
        }

        @Override
        public CompletionStage<Void> ack(Metadata metadata) {
            return delegate.ack(metadata);
        }

        @Override
        public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
            return delegate.nack(reason, metadata);
        }

        @Override
        public synchronized void injectMetadata(Object metadataObject) {
            metadata = metadata.with(metadataObject);
            if (delegate instanceof MetadataInjectableMessage<?> injectable) {
                injectable.injectMetadata(metadataObject);
            }
        }

        @Override
        public <C> C unwrap(Class<C> unwrapType) {
            if (unwrapType.isInstance(this)) {
                return unwrapType.cast(this);
            }
            return delegate.unwrap(unwrapType);
        }
    }

    private static class TestMessages<T> implements Messages<T> {

        private final List<org.apache.pulsar.client.api.Message<T>> messages;

        TestMessages(List<org.apache.pulsar.client.api.Message<T>> messages) {
            this.messages = messages;
        }

        @Override
        public int size() {
            return messages.size();
        }

        @Override
        public Iterator<org.apache.pulsar.client.api.Message<T>> iterator() {
            return messages.iterator();
        }
    }
}
