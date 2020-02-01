package io.smallrye.reactive.messaging.extension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.*;

/**
 * Implementation of the emitter pattern.
 *
 * @param <T> the type of payload sent by the emitter.
 */
public class EmitterImpl<T> implements Emitter<T> {

    private final AtomicReference<FlowableEmitter<Message<? extends T>>> internal = new AtomicReference<>();
    private final Flowable<Message<? extends T>> publisher;

    private static final Logger LOGGER = LoggerFactory.getLogger(EmitterImpl.class);
    private final String name;

    EmitterImpl(String name, String overFlowStrategy, long bufferSize, long defaultBufferSize) {
        this.name = name;
        if (defaultBufferSize <= 0) {
            throw new IllegalArgumentException("The default buffer size must be strictly positive");
        }

        FlowableOnSubscribe<Message<? extends T>> deferred = fe -> {
            if (!internal.compareAndSet(null, fe.serialize())) {
                fe.onError(new Exception("Emitter already created"));
            }
        };

        System.out.println("found strategy " + overFlowStrategy);
        if (overFlowStrategy == null) {
            publisher = getPublisherUsingBufferStrategy(name, defaultBufferSize,
                    Flowable.create(deferred, BackpressureStrategy.BUFFER));
        } else {
            publisher = getPublisherForStrategy(name, overFlowStrategy, bufferSize, defaultBufferSize, deferred);
        }
    }

    static <T> Flowable<Message<? extends T>> getPublisherForStrategy(String name, String overFlowStrategy, long bufferSize,
            long defaultBufferSize,
            FlowableOnSubscribe<Message<? extends T>> deferred) {
        System.out.println("found strategy " + overFlowStrategy);
        OnOverflow.Strategy strategy = OnOverflow.Strategy.valueOf(overFlowStrategy);

        switch (strategy) {
            case BUFFER:
                Flowable<Message<? extends T>> p = Flowable.create(deferred, BackpressureStrategy.BUFFER);
                if (bufferSize > 0) {
                    return getPublisherUsingBufferStrategy(name, bufferSize, p);
                } else {
                    return getPublisherUsingBufferStrategy(name, defaultBufferSize, p);
                }

            case UNBOUNDED_BUFFER:
                return Flowable.create(deferred, BackpressureStrategy.BUFFER);

            case DROP:
                return Flowable.create(deferred, BackpressureStrategy.DROP);

            case FAIL:
                return Flowable.create(deferred, BackpressureStrategy.ERROR);

            case LATEST:
                return Flowable.create(deferred, BackpressureStrategy.LATEST);

            case NONE:
                return Flowable.create(deferred, BackpressureStrategy.MISSING);

            default:
                throw new IllegalArgumentException("Invalid back pressure strategy: " + overFlowStrategy);
        }
    }

    /**
     * Creates the stream when using the default buffer size.
     *
     * @param name the name of the emitter
     * @param defaultBufferSize the default buffer size
     * @param stream the upstream
     * @param <T> the type of payload
     * @return the stream.
     */
    static <T> Flowable<Message<? extends T>> getPublisherUsingBufferStrategy(String name,
            long defaultBufferSize,
            Flowable<Message<? extends T>> stream) {
        System.out.println("with buffer " + defaultBufferSize);
        return stream.onBackpressureBuffer(defaultBufferSize - 2, // RX Implementation details
                () -> LOGGER.error("Buffer full for emitter {}", name), BackpressureOverflowStrategy.ERROR);
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
        FlowableEmitter<Message<? extends T>> emitter = verify(internal, name);
        CompletableFuture<Void> future = new CompletableFuture<>();
        emitter.onNext(Message.of(msg, () -> {
            future.complete(null);
            return future;
        }));
        return future;
    }

    @Override
    public synchronized <M extends Message<? extends T>> void send(M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("`null` is not a valid value");
        }
        FlowableEmitter<Message<? extends T>> emitter = verify(internal, name);
        emitter.onNext(msg);

    }

    static <T> FlowableEmitter<Message<? extends T>> verify(AtomicReference<FlowableEmitter<Message<? extends T>>> reference,
            String name) {
        FlowableEmitter<Message<? extends T>> emitter = reference.get();
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
        verify(internal, name).onComplete();
    }

    @Override
    public synchronized void error(Exception e) {
        if (e == null) {
            throw new IllegalArgumentException("`null` is not a valid exception");
        }
        verify(internal, name).onError(e);

    }

    @Override
    public synchronized boolean isCancelled() {
        FlowableEmitter<Message<? extends T>> emitter = internal.get();
        return emitter == null || emitter.isCancelled();
    }

    @Override
    public boolean isRequested() {
        FlowableEmitter<Message<? extends T>> emitter = internal.get();
        return !isCancelled() && emitter.requested() > 0;
    }
}
