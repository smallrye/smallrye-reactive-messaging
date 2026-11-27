package io.smallrye.reactive.messaging.providers.extension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;

/**
 * A message delivered by a channel tracking its in-flight messages for graceful shutdown.
 * <p>
 * It delegates the payload, the metadata, acknowledgement and negative acknowledgement to the original message. The first of
 * them to complete, or to fail, marks the message as processed once.
 * <p>
 * Like the copies made by {@link Message#withAckWithMetadata(Function)}, this replaces the connector's message type:
 * {@code instanceof} checks see a {@code TrackedMessage}, as they see the copies made by observation or interceptors.
 * Outbound connectors therefore build what they send from the payload and the metadata only.
 * <p>
 * A single object per message replaces the two copies, the two ack and nack functions and the processed flag
 * that the previous implementation allocated. When the acknowledgement of the original message is already
 * completed, which is the usual case for synchronous acknowledgements, no stage is added either.
 */
final class TrackedMessage<T> implements MetadataInjectableMessage<T> {

    private static final AtomicIntegerFieldUpdater<TrackedMessage> PROCESSED = AtomicIntegerFieldUpdater
            .newUpdater(TrackedMessage.class, "processed");

    private final Message<T> delegate;
    // Metadata injected into this message when the original does not accept injected metadata, null if none.
    private volatile Metadata injected;
    private final Runnable onProcessed;
    private volatile int processed;

    /**
     * @param delegate the message to track
     * @param onProcessed called once, when the message is acknowledged or negatively acknowledged
     */
    TrackedMessage(Message<T> delegate, Runnable onProcessed) {
        this.delegate = delegate;
        this.onProcessed = onProcessed;
    }

    @Override
    public T getPayload() {
        return delegate.getPayload();
    }

    @Override
    public Metadata getMetadata() {
        Metadata metadata = delegate.getMetadata();
        Metadata extra = injected;
        if (extra != null) {
            for (Object item : extra) {
                metadata = metadata.with(item);
            }
        }
        return metadata;
    }

    @Override
    public Function<Metadata, CompletionStage<Void>> getAckWithMetadata() {
        // Used when the message is copied (withPayload, withMetadata...): the copy acknowledges through this message.
        return this::ack;
    }

    @Override
    public BiFunction<Throwable, Metadata, CompletionStage<Void>> getNackWithMetadata() {
        return this::nack;
    }

    @Override
    public CompletionStage<Void> ack(Metadata metadata) {
        CompletionStage<Void> stage;
        try {
            stage = delegate.ack(metadata);
        } catch (RuntimeException e) {
            markProcessed();
            throw e;
        }
        return track(stage);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata metadata) {
        if (reason == null) {
            // Same contract as Message.nack, and the message is not processed.
            throw new IllegalArgumentException("The reason must not be `null`");
        }
        CompletionStage<Void> stage;
        try {
            stage = delegate.nack(reason, metadata);
        } catch (RuntimeException e) {
            markProcessed();
            throw e;
        }
        return track(stage);
    }

    /**
     * Returns this message if it is an instance of the given type, otherwise unwraps the original message, so the
     * connector message type, and the types the connector message unwraps to, stay reachable.
     * <p>
     * Caveat: acknowledging the unwrapped original message, or negatively acknowledging it, bypasses the tracking:
     * the message stays in flight until the drain timeout of the graceful shutdown. Acknowledge the message received
     * by the method instead.
     *
     * @param unwrapType the type to unwrap to
     * @return this message, or the original message unwrapped to the type
     * @throws IllegalArgumentException if the type is {@code null}, or if the original message cannot be unwrapped
     *         to it
     */
    @Override
    public <C> C unwrap(Class<C> unwrapType) {
        if (unwrapType != null && unwrapType.isInstance(this)) {
            return unwrapType.cast(this);
        }
        return delegate.unwrap(unwrapType);
    }

    /**
     * Adds the metadata object to the original message if it accepts injected metadata, so the features injecting
     * metadata before acknowledgement (transactions, tracing) reach the connector. Otherwise, it is kept on this
     * message, on top of the original's metadata.
     */
    @Override
    public void injectMetadata(Object metadataObject) {
        if (delegate instanceof MetadataInjectableMessage<T> injectable) {
            injectable.injectMetadata(metadataObject);
        } else {
            synchronized (this) {
                Metadata extra = injected;
                injected = extra == null ? Metadata.of(metadataObject) : extra.with(metadataObject);
            }
        }
    }

    private CompletionStage<Void> track(CompletionStage<Void> stage) {
        if (stage == null) {
            markProcessed();
            return CompletableFuture.completedFuture(null);
        }
        if (stage instanceof CompletableFuture<Void> future && future.isDone()) {
            // Already completed, successfully or not: no need to add a stage.
            markProcessed();
            return stage;
        }
        return stage.whenComplete((v, t) -> markProcessed());
    }

    private void markProcessed() {
        if (PROCESSED.compareAndSet(this, 0, 1)) {
            onProcessed.run();
        }
    }

    /**
     * @return whether the message has been acknowledged or negatively acknowledged
     */
    boolean isProcessed() {
        return processed == 1;
    }

    /**
     * Describes the original message: the tracking is transparent, so logs that print the message, such as the
     * connectors' sent and received messages, show the original rather than the wrapper.
     */
    @Override
    public String toString() {
        return delegate.toString();
    }
}
