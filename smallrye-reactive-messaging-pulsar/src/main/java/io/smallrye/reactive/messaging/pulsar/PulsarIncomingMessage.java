package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage.captureContextMetadata;
import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarMessages.msg;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;

public class PulsarIncomingMessage<T> implements PulsarMessage<T>, PulsarIdMessage<T>, MetadataInjectableMessage<T> {
    private final org.apache.pulsar.client.api.Message<T> delegate;
    private Metadata metadata;

    private final PulsarAckHandler ackHandler;

    private final PulsarFailureHandler nackHandler;

    public PulsarIncomingMessage(Message<T> message, PulsarAckHandler ackHandler, PulsarFailureHandler nackHandler) {
        this.delegate = Objects.requireNonNull(message, msg.isRequired("message"));
        this.ackHandler = Objects.requireNonNull(ackHandler, msg.isRequired("ack"));
        this.nackHandler = Objects.requireNonNull(nackHandler, msg.isRequired("nack"));
        this.metadata = captureContextMetadata(new PulsarIncomingMessageMetadata(message));
    }

    @Override
    public MessageId getMessageId() {
        return delegate.getMessageId();
    }

    @Override
    public T getPayload() {
        return delegate.getValue();
    }

    @Override
    public String getKey() {
        return delegate.getKey();
    }

    @Override
    public byte[] getKeyBytes() {
        return delegate.getKeyBytes();
    }

    @Override
    public boolean hasKey() {
        return delegate.hasKey();
    }

    @Override
    public byte[] getOrderingKey() {
        return delegate.getOrderingKey();
    }

    @Override
    public Map<String, String> getProperties() {
        return delegate.getProperties();
    }

    @Override
    public long getEventTime() {
        return delegate.getEventTime();
    }

    @Override
    public long getSequenceId() {
        return delegate.getSequenceId();
    }

    public long getPublishTime() {
        return delegate.getPublishTime();
    }

    public org.apache.pulsar.client.api.Message<T> unwrap() {
        return delegate;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack() {
        return ackHandler.handle(this).subscribeAsCompletionStage();
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this::ack;
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        return nackHandler.handle(this, reason, metadata).subscribeAsCompletionStage();
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

    @Override
    public synchronized void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
