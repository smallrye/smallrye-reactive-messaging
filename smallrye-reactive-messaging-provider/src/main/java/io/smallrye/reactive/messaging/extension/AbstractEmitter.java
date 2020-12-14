package io.smallrye.reactive.messaging.extension;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.helpers.BroadcastHelper;

public abstract class AbstractEmitter<T> {
    protected final AtomicReference<MultiEmitter<? super Message<? extends T>>> internal = new AtomicReference<>();
    protected final Multi<Message<? extends T>> publisher;

    protected final String name;

    protected final AtomicReference<Throwable> synchronousFailure = new AtomicReference<>();

    @SuppressWarnings("unchecked")
    public AbstractEmitter(EmitterConfiguration config, long defaultBufferSize) {
        this.name = config.name;
        if (defaultBufferSize <= 0) {
            throw ex.illegalArgumentForDefaultBuffer();
        }

        Consumer<MultiEmitter<? super Message<? extends T>>> deferred = fe -> {
            MultiEmitter<? super Message<? extends T>> previous = internal.getAndSet(fe);
            if (previous != null) {
                previous.complete();
            }
        };

        Multi<Message<? extends T>> tempPublisher;
        if (config.overflowBufferStrategy == null) {
            Multi<Message<? extends T>> multi = Multi.createFrom().emitter(deferred, BackPressureStrategy.BUFFER);
            tempPublisher = getPublisherUsingBufferStrategy(defaultBufferSize, multi);
        } else {
            tempPublisher = getPublisherForStrategy(config.overflowBufferStrategy, config.overflowBufferSize,
                    defaultBufferSize, deferred);
        }

        if (config.broadcast) {
            publisher = (Multi<Message<? extends T>>) BroadcastHelper
                    .broadcastPublisher(tempPublisher, config.numberOfSubscriberBeforeConnecting).buildRs();
        } else {
            publisher = tempPublisher;
        }
    }

    public synchronized void complete() {
        verify(internal, name).complete();
    }

    public synchronized void error(Exception e) {
        if (e == null) {
            throw ex.illegalArgumentForException("null");
        }
        verify(internal, name).fail(e);
    }

    public synchronized boolean isCancelled() {
        MultiEmitter<? super Message<? extends T>> emitter = internal.get();
        return emitter == null || emitter.isCancelled();
    }

    public boolean hasRequests() {
        MultiEmitter<? super Message<? extends T>> emitter = internal.get();
        return !isCancelled() && emitter.requested() > 0;
    }

    Multi<Message<? extends T>> getPublisherForStrategy(OnOverflow.Strategy overFlowStrategy, long bufferSize,
            long defaultBufferSize,
            Consumer<MultiEmitter<? super Message<? extends T>>> deferred) {
        switch (overFlowStrategy) {
            case BUFFER:
                if (bufferSize > 0) {
                    return ThrowingEmitter.create(deferred, bufferSize);
                } else {
                    return ThrowingEmitter.create(deferred, defaultBufferSize);
                }

            case UNBOUNDED_BUFFER:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.BUFFER);

            case THROW_EXCEPTION:
                return ThrowingEmitter.create(deferred, 0);

            case DROP:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.DROP);

            case FAIL:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.ERROR);

            case LATEST:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.LATEST);

            case NONE:
                return Multi.createFrom().emitter(deferred, BackPressureStrategy.IGNORE);

            default:
                throw ex.illegalArgumentForBackPressure(overFlowStrategy);
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
                .onOverflow().buffer(size - 2)
                .onFailure().invoke(t -> synchronousFailure.set(t));
    }

    public Publisher<Message<? extends T>> getPublisher() {
        return publisher;
    }

    boolean isSubscribed() {
        return internal.get() != null;
    }

    protected synchronized void emit(Message<? extends T> message) {
        if (message == null) {
            throw ex.illegalArgumentForNullValue();
        }

        MultiEmitter<? super Message<? extends T>> emitter = verify(internal, name);
        if (synchronousFailure.get() != null) {
            throw ex.illegalStateForEmitter(synchronousFailure.get());
        }
        if (emitter.isCancelled()) {
            throw ex.illegalStateForDownstreamCancel();
        }
        emitter.emit(message);
        if (synchronousFailure.get() != null) {
            throw ex.illegalStateForEmitterWhileEmitting(synchronousFailure.get());
        }
    }

    static <T> MultiEmitter<? super Message<? extends T>> verify(
            AtomicReference<MultiEmitter<? super Message<? extends T>>> reference,
            String name) {
        MultiEmitter<? super Message<? extends T>> emitter = reference.get();
        if (emitter == null) {
            throw ex.illegalStateForNoSubscriber(name);
        }
        if (emitter.isCancelled()) {
            throw ex.illegalStateForCancelledSubscriber(name);
        }
        return emitter;
    }
}
