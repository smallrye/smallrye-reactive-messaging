package io.smallrye.reactive.messaging.impl;

import org.eclipse.microprofile.reactive.messaging.Message;
import io.smallrye.reactive.messaging.MessageBuilder;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public final class MessageBuilderImpl<T> implements MessageBuilder<T> {
    private T payload;
    private Metadata metadata = Metadata.empty();
    private Supplier<CompletionStage<Void>> ackSupplier;

    @Override
    public <P> MessageBuilder<T> of(Message<P> message) {
        this.payload = (T) message.getPayload();
        this.metadata = message.getMetadata();
        this.ackSupplier = message.getAck();
        return this;
    }

    @Override
    public MessageBuilder<T> payload(T payload) {
        this.payload = payload;
        return this;
    }

    @Override
    public MessageBuilder<T> metadata(Metadata metadata) {
        this.metadata = Metadata.from(metadata);
        return this;
    }

    @Override
    public MessageBuilder<T> metadata(Iterable metadata) {
        this.metadata = Metadata.from(metadata);
        return this;
    }

    @Override
    public MessageBuilder<T> metadata(Object... metadata) {
        this.metadata = Metadata.of(metadata);
        return this;
    }

    @Override
    public MessageBuilder<T> addMetadata(Object... metadata) {
        for(Object m:metadata) {
            this.metadata = this.metadata.with(m);
        }
        return this;
    }

    @Override
    public MessageBuilder<T> ack(Supplier<CompletionStage<Void>> ackSupplier) {
        this.ackSupplier = ackSupplier;
        return this;
    }

    @Override
    public Message<T> build() {
        return new MessageImpl<>(payload, metadata, ackSupplier);
    }
}
