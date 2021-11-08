package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.reactive.messaging.providers.i18n.ProviderExceptions.ex;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * An Emitter which throws an exception if asked to emit when there's insufficient requests from downstream.
 * <p>
 * Can be configured with a buffer to allow a certain number of items to be emitted without downstream requests.
 *
 * @param <T> the type to emit
 */
class ThrowingEmitter<T> implements MultiEmitter<T> {

    private MultiEmitter<? super T> delegate;
    private final AtomicLong requested;

    public static <T> Multi<T> create(Consumer<MultiEmitter<? super T>> deferred, long bufferSize) {
        // ThrowingEmitter works by wrapping around a delegate emitter and tracking the requests from downstream so that it can throw an exception from emit() if there aren't sufficient requests

        // If there's no buffer we can use IGNORE since we do our own counting of requests, otherwise we need the delegate to buffer requests for us
        BackPressureStrategy backPressureStrategy = bufferSize == 0 ? BackPressureStrategy.IGNORE : BackPressureStrategy.BUFFER;

        // Use deferred so that we can add a separate on request callback for each subscriber
        return Multi.createFrom().deferred(() -> {

            ThrowingEmitter<T> throwingEmitter = new ThrowingEmitter<>(bufferSize);

            // When someone subscribes, wrap the emitter with our throwing emitter
            Consumer<MultiEmitter<? super T>> consumer = emitter -> {
                throwingEmitter.delegate = emitter;
                deferred.accept(throwingEmitter);
            };

            // Create the Multi and attach the request callback
            return Multi.createFrom().emitter(consumer, backPressureStrategy)
                    .onRequest().invoke(throwingEmitter::request);
        });
    }

    public ThrowingEmitter(long bufferSize) {
        requested = new AtomicLong(bufferSize);
    }

    public MultiEmitter<T> emit(T item) {
        // Decrement requested without going below zero
        long requests;
        do {
            requests = requested.get();
        } while (requests > 0 && !requested.compareAndSet(requests, requests - 1));

        if (requests <= 0) {
            throw ex.illegalStateInsufficientDownstreamRequests();
        }

        delegate.emit(item);
        return this;
    }

    public void fail(Throwable failure) {
        delegate.fail(failure);
    }

    public void complete() {
        delegate.complete();
    }

    public MultiEmitter<T> onTermination(Runnable onTermination) {
        delegate.onTermination(onTermination);
        return this;
    }

    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    public long requested() {
        return delegate.requested();
    }

    public void request(long requests) {
        Subscriptions.add(requested, requests);
    }
}
