package io.smallrye.reactive.messaging.extension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * Implementation of the emitter pattern.
 *
 * @param <T> the type of payload sent by the emitter.
 */
public class EmitterImpl<T> implements Emitter<T> {

    private final AtomicReference<MultiEmitter<? super Message<? extends T>>> internal = new AtomicReference<>();
    private final Multi<Message<? extends T>> publisher;

    private final String name;

    private AtomicReference<Throwable> synchronousFailure = new AtomicReference<>();

    public EmitterImpl(String name, String overFlowStrategy, long bufferSize, long defaultBufferSize) {
        this.name = name;
        if (defaultBufferSize <= 0) {
            throw new IllegalArgumentException("The default buffer size must be strictly positive");
        }

        Consumer<MultiEmitter<? super Message<? extends T>>> deferred = fe -> {
            MultiEmitter<? super Message<? extends T>> previous = internal.getAndSet(fe);
            if (previous != null) {
                previous.complete();
            }
        };

        if (overFlowStrategy == null) {
            Multi<Message<? extends T>> multi = Multi.createFrom().emitter(deferred, BackPressureStrategy.BUFFER);
            publisher = getPublisherUsingBufferStrategy(defaultBufferSize, multi);
        } else {
            publisher = getPublisherForStrategy(overFlowStrategy, bufferSize, defaultBufferSize, deferred);
        }
    }

    Multi<Message<? extends T>> getPublisherForStrategy(String overFlowStrategy, long bufferSize,
            long defaultBufferSize,
            Consumer<MultiEmitter<? super Message<? extends T>>> deferred) {
        OnOverflow.Strategy strategy = OnOverflow.Strategy.valueOf(overFlowStrategy);

        switch (strategy) {
            case BUFFER:
                Multi<Message<? extends T>> multi = Multi.createFrom().emitter(deferred, BackPressureStrategy.BUFFER);
                if (bufferSize > 0) {
                    return getPublisherUsingBufferStrategy(bufferSize, multi);
                } else {
                    return getPublisherUsingBufferStrategy(defaultBufferSize, multi);
                }

            case UNBOUNDED_BUFFER:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.BUFFER);

            case DROP:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.DROP);

            case FAIL:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.ERROR);

            case LATEST:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.LATEST);

            case NONE:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.IGNORE);

            default:
                throw new IllegalArgumentException("Invalid back-pressure strategy: " + overFlowStrategy);
        }
    }

    /**
     * Creates the stream when using the default buffer size.
     *
     * @param defaultBufferSize the default buffer size
     * @param stream the upstream
     * @return the stream.
     */
    Multi<Message<? extends T>> getPublisherUsingBufferStrategy(long defaultBufferSize,
            Multi<Message<? extends T>> stream) {
        int size = (int) defaultBufferSize;
        return stream
                .on().overflow().buffer(size - 2)
                .onFailure().invoke(t -> synchronousFailure.set(t));
    }

    public Publisher<Message<? extends T>> getPublisher() {
        return publisher;
    }

    boolean isSubscribed() {
        return internal.get() != null;
    }

    @Override
    public synchronized CompletionStage<Void> send(T msg) {
        if (msg == null) {
            throw new IllegalArgumentException("`null` is not a valid value");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        emit(Message.of(msg, () -> {
            future.complete(null);
            return future;
        }));
        return future;
    }

    private synchronized void emit(Message<? extends T> message) {
        MultiEmitter<? super Message<? extends T>> emitter = verify(internal, name);
        if (synchronousFailure.get() != null) {
            throw new IllegalStateException("The emitter encountered a failure", synchronousFailure.get());
        }
        if (emitter.isCancelled()) {
            throw new IllegalStateException("The downstream has cancelled the consumption");
        }
        emitter.emit(message);
        if (synchronousFailure.get() != null) {
            throw new IllegalStateException("The emitter encountered a failure while emitting", synchronousFailure.get());
        }
    }

    @Override
    public synchronized <M extends Message<? extends T>> void send(M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("`null` is not a valid value");
        }
        emit(msg);
    }

    static <T> MultiEmitter<? super Message<? extends T>> verify(
            AtomicReference<MultiEmitter<? super Message<? extends T>>> reference,
            String name) {
        MultiEmitter<? super Message<? extends T>> emitter = reference.get();
        if (emitter == null) {
            throw new IllegalStateException("No subscriber found for the channel " + name);
        }
        if (emitter.isCancelled()) {
            throw new IllegalStateException("The subscription to " + name + " has been cancelled");
        }
        return emitter;
    }

    @Override
    public synchronized void complete() {
        verify(internal, name).complete();
    }

    @Override
    public synchronized void error(Exception e) {
        if (e == null) {
            throw new IllegalArgumentException("`null` is not a valid exception");
        }
        verify(internal, name).fail(e);
    }

    @Override
    public synchronized boolean isCancelled() {
        MultiEmitter<? super Message<? extends T>> emitter = internal.get();
        return emitter == null || emitter.isCancelled();
    }

    @Override
    public boolean isRequested() {
        MultiEmitter<? super Message<? extends T>> emitter = internal.get();
        return !isCancelled() && emitter.requested() > 0;
    }
}
