package io.smallrye.reactive.messaging.impl;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

public final class MessageImpl<T> implements Message<T> {
    private T payload;
    private Metadata metadata;
    private Supplier<CompletionStage<Void>> ack;

    MessageImpl(T payload, Metadata metadata, Supplier<CompletionStage<Void>> ack) {
        this.payload = payload;
        this.metadata = metadata;
        this.ack = ack;
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public <M> Optional<M> getMetadata(Class<? extends M> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("`clazz` must not be `null`");
        }
        return StreamSupport.stream(getMetadata().spliterator(), false)
            .filter(clazz::isInstance)
            .map(x -> (M) x) // casting is safe here as we checked the type before.
            .findAny();
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return ack;
    }

    @Override
    public CompletionStage<Void> ack() {
        Supplier<CompletionStage<Void>> ack = getAck();
        if (ack == null) {
            return CompletableFuture.completedFuture(null);
        } else {
            return ack.get();
        }
    }
}
