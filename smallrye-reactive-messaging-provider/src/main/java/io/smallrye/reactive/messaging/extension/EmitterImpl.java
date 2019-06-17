package io.smallrye.reactive.messaging.extension;

import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.BackpressureOverflowStrategy;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.smallrye.reactive.messaging.annotations.Emitter;

public class EmitterImpl<T> implements Emitter<T> {

    private final AtomicReference<FlowableEmitter<Message<? extends T>>> internal = new AtomicReference<>();
    private final Flowable<Message<? extends T>> publisher;
    private final String name;

    private static final Logger LOGGER = LoggerFactory.getLogger(EmitterImpl.class);

    EmitterImpl(String name) {
        this.name = name;
        publisher = Flowable.<Message<? extends T>> create(fe -> {
            if (!internal.compareAndSet(null, fe)) {
                fe.onError(new Exception("Emitter already created"));
            }
        }, BackpressureStrategy.BUFFER)
                .onBackpressureBuffer(128, () -> LOGGER.error("Buffer full for emitter {}", name),
                        BackpressureOverflowStrategy.ERROR);

    }

    public Publisher<Message<? extends T>> getPublisher() {
        return publisher;
    }

    boolean isConnected() {
        return internal.get() != null;
    }

    @Override
    public synchronized Emitter<T> send(T msg) {
        if (msg == null) {
            throw new IllegalArgumentException("`null` is not a valid value");
        }
        FlowableEmitter<Message<? extends T>> emitter = verify();
        if (msg instanceof Message) {
            //noinspection unchecked
            emitter.onNext((Message) msg);
        } else {
            emitter.onNext(Message.of(msg));
        }
        return this;
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
