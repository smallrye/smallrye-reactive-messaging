package io.smallrye.reactive.messaging;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.mutiny.helpers.Subscriptions;

public class SubscriberWrapper<I, T> implements Processor<T, T> {

    /**
     * The subscriber provided by the user.
     */
    private final Subscriber<I> delegate;
    private final BiFunction<T, Throwable, CompletionStage<Void>> postAck;

    private final AtomicReference<Subscription> upstream = new AtomicReference<>();

    private final Function<T, I> mapper;

    public SubscriberWrapper(Subscriber<I> userSubscriber, Function<T, I> mapper,
            BiFunction<T, Throwable, CompletionStage<Void>> postAck) {
        this.delegate = Objects.requireNonNull(userSubscriber);
        this.mapper = Objects.requireNonNull(mapper);
        this.postAck = postAck;
    }

    /**
     * Gets called with the downstream subscriber (from reactive messaging).
     *
     * @param s the downstream subscriber
     */
    @Override
    public void subscribe(Subscriber<? super T> s) {
        s.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                // ignore requests
            }

            @Override
            public void cancel() {
                // cancel subscription upstream
                Subscription subscription = upstream.getAndSet(Subscriptions.CANCELLED);
                if (subscription != null) {
                    subscription.cancel();
                }
            }
        });
    }

    /**
     * Receives the subscription from the upstream.
     *
     * @param s the subscription
     */
    @Override
    public void onSubscribe(Subscription s) {

        if (!upstream.compareAndSet(null, s)) {
            throw new IllegalStateException("We already received a subscription");
        }

        // Pass the subscription to the user subscriber.
        delegate.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                s.request(n);
            }

            @Override
            public void cancel() {
                Subscription subscription = upstream.getAndSet(Subscriptions.CANCELLED);
                if (subscription != null) {
                    subscription.cancel();
                }
            }
        });
    }

    @Override
    public void onNext(T item) {
        try {
            delegate.onNext(mapper.apply(item));
            if (postAck != null) {
                postAck.apply(item, null);
            }
        } catch (Exception e) {
            if (postAck != null) {
                postAck.apply(item, e);
            }
        }
    }

    @Override
    public void onError(Throwable error) {
        delegate.onError(error);
    }

    @Override
    public void onComplete() {
        delegate.onComplete();
    }
}
