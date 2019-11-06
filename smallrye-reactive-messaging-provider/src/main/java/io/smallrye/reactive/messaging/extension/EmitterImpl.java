package io.smallrye.reactive.messaging.extension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.*;
import io.smallrye.reactive.messaging.Emitter;
import io.smallrye.reactive.messaging.annotations.OnOverflow;

public class EmitterImpl<T> implements Emitter<T> {

    private final AtomicReference<FlowableEmitter<Message<? extends T>>> internal = new AtomicReference<>();
    private final Flowable<Message<? extends T>> publisher;

    private static final Logger LOGGER = LoggerFactory.getLogger(EmitterImpl.class);

    EmitterImpl(String name, String overFlowStrategy, long bufferSize, long defaultBufferSize) {
        FlowableOnSubscribe<Message<? extends T>> deferred = fe -> {
            if (!internal.compareAndSet(null, fe)) {
                fe.onError(new Exception("Emitter already created"));
            }
        };
        if (overFlowStrategy == null) {
            publisher = Flowable.create(deferred, BackpressureStrategy.BUFFER).onBackpressureBuffer(defaultBufferSize,
                    () -> LOGGER.error("Buffer full for emitter {}", name), BackpressureOverflowStrategy.ERROR);
        } else {
            OnOverflow.Strategy strategy = OnOverflow.Strategy.valueOf(overFlowStrategy);
            switch (strategy) {
                case BUFFER:
                    Flowable<Message<? extends T>> p = Flowable.create(deferred, BackpressureStrategy.BUFFER);
                    if (bufferSize > 0) {
                        publisher = p.onBackpressureBuffer(bufferSize,
                                () -> LOGGER.error("Buffer full for emitter {}", name), BackpressureOverflowStrategy.ERROR);
                    } else {
                        publisher = p.onBackpressureBuffer(defaultBufferSize,
                                () -> LOGGER.error("Buffer full for emitter {}", name), BackpressureOverflowStrategy.ERROR);

                    }
                    break;
                case UNBOUNDED_BUFFER:
                    publisher = Flowable.create(deferred, BackpressureStrategy.BUFFER);
                    break;
                case DROP:
                    publisher = Flowable.create(deferred, BackpressureStrategy.DROP);
                    break;
                case FAIL:
                    publisher = Flowable.create(deferred, BackpressureStrategy.ERROR);
                    break;
                case LATEST:
                    publisher = Flowable.create(deferred, BackpressureStrategy.LATEST);
                    break;
                case NONE:
                    publisher = Flowable.create(deferred, BackpressureStrategy.MISSING);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid back pressure strategy: " + overFlowStrategy);
            }
        }
    }

    public Publisher<Message<? extends T>> getPublisher() {
        return publisher;
    }

    boolean isConnected() {
        return internal.get() != null;
    }

    @Override
    public synchronized CompletionStage<Void> send(T msg) {
        if (msg == null) {
            throw new IllegalArgumentException("`null` is not a valid value");
        }
        FlowableEmitter<Message<? extends T>> emitter = verify();
        if (msg instanceof Message) {
            //noinspection unchecked
            return CompletableFuture.runAsync(() -> emitter.onNext((Message) msg));
        } else {
            return CompletableFuture.runAsync(() -> emitter.onNext(Message.of(msg)));
        }

    }

    private synchronized FlowableEmitter<Message<? extends T>> verify() {
        FlowableEmitter<Message<? extends T>> emitter = internal.get();
        if (emitter == null) {
            throw new IllegalStateException("Stream not yet connected");
        }
        if (emitter.isCancelled()) {
            throw new IllegalStateException("Stream has been terminated");
        }
        return emitter;
    }

    @Override
    public synchronized void complete() {
        verify().onComplete();
    }

    @Override
    public synchronized void error(Exception e) {
        if (e == null) {
            throw new IllegalArgumentException("`null` is not a valid exception");
        }
        verify().onError(e);
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
