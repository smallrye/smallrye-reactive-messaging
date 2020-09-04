package io.smallrye.reactive.messaging;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class SubscriberWrapper<I, T> implements Processor<T, T> {

    /**
     * The subscriber provided by the user.
     */
    private final Subscriber<I> delegate;
    private final BiFunction<T, Throwable, CompletionStage<Void>> postAck;

    /**
     * The downstream subscriber.
     */
    private final AtomicReference<Subscriber<? super T>> subscriber = new AtomicReference<>();

    private final Function<T, I> mapper;

    public SubscriberWrapper(Subscriber<I> subscriber, Function<T, I> mapper,
            BiFunction<T, Throwable, CompletionStage<Void>> postAck) {
        this.delegate = Objects.requireNonNull(subscriber);
        this.mapper = Objects.requireNonNull(mapper);
        this.postAck = postAck;
    }

    /**
     * Gets call with the downstream subscriber (from reactive messaging)
     *
     * @param s the downstream subscriber
     */
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (!this.subscriber.compareAndSet(null, s)) {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    // Ignored.
                }

                @Override
                public void cancel() {
                    // Ignored.
                }
            });
            s.onError(ex.illegalStateForNotSupported("Broadcast"));
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        // Pass a custom subscription to the downstream
        subscriber.get().onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                // ignore requests
            }

            @Override
            public void cancel() {
                // cancel subscription upstream
                s.cancel();
            }
        });

        // Pass the subscription to the user subscriber.
        delegate.onSubscribe(s);
    }

    @Override
    public void onNext(T item) {
        try {
            delegate.onNext(mapper.apply(item));
            if (postAck != null) {
                postAck.apply(item, null).thenAccept(x -> subscriber.get().onNext(item));
            } else {
                subscriber.get().onNext(item);
            }
        } catch (Exception e) {
            if (postAck != null) {
                postAck.apply(item, e).thenAccept(x -> subscriber.get().onNext(item));
            } else {
                subscriber.get().onError(e);
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
