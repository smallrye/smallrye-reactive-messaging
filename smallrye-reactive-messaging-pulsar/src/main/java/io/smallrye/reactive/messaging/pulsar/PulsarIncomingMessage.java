package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarMessages.msg;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

class PulsarIncomingMessage<T> implements Message<T> {
    private final org.apache.pulsar.client.api.Message<T> delegate;
    private final Acknowledgement acknowledgement;
    private final Metadata metadata;

    public PulsarIncomingMessage(org.apache.pulsar.client.api.Message<T> message,
            Acknowledgement acknowledgement) {
        this.delegate = Objects.requireNonNull(message, msg.isRequired("message"));
        this.acknowledgement = Objects.requireNonNull(acknowledgement, msg.isRequired("consumer"));
        this.metadata = Metadata.of(new PulsarIncomingMessageMetadata(message));
    }

    @Override
    public T getPayload() {
        return delegate.getValue();
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack() {
        return acknowledgement.ack(delegate);
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason) {
        return acknowledgement.nack(delegate, reason);
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this::nack;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PulsarIncomingMessage<?> that = (PulsarIncomingMessage<?>) o;
        return delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }
}
