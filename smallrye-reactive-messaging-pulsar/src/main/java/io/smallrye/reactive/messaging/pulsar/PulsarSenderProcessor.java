package io.smallrye.reactive.messaging.pulsar;

import static io.smallrye.reactive.messaging.pulsar.i18n.PulsarExceptions.ex;

import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.Subscriptions;

class PulsarSenderProcessor implements Processor<Message<?>, Message<?>>, Subscription {

    private final long inflights;
    private final boolean waitForCompletion;
    private final Function<Message<?>, Uni<Void>> send;
    private final AtomicReference<Subscription> subscription = new AtomicReference<>();
    private final AtomicReference<Subscriber<? super Message<?>>> downstream = new AtomicReference<>();

    public PulsarSenderProcessor(long inflights, boolean waitForCompletion, Function<Message<?>, Uni<Void>> send) {
        this.inflights = inflights;
        this.waitForCompletion = waitForCompletion;
        this.send = send;
    }

    @Override
    public void subscribe(
            Subscriber<? super Message<?>> subscriber) {
        if (!downstream.compareAndSet(null, subscriber)) {
            Subscriptions.fail(subscriber, ex.illegalStateOnlyOneSubscriber());
        } else {
            if (subscription.get() != null) {
                subscriber.onSubscribe(this);
            }
        }
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        if (this.subscription.compareAndSet(null, subscription)) {
            Subscriber<? super Message<?>> subscriber = downstream.get();
            if (subscriber != null) {
                subscriber.onSubscribe(this);
            }
        } else {
            Subscriber<? super Message<?>> subscriber = downstream.get();
            if (subscriber != null) {
                subscriber.onSubscribe(Subscriptions.CANCELLED);
            }
        }
    }

    @Override
    public void onNext(Message<?> message) {
        if (waitForCompletion) {
            send.apply(message)
                    .subscribe().with(
                            x -> requestNext(message),
                            this::onError);
        } else {
            send.apply(message)
                    .subscribe().with(x -> {
                    }, this::onError);
            requestNext(message);
        }
    }

    @Override
    public void request(long l) {
        if (l != Long.MAX_VALUE) {
            throw ex.illegalStateConsumeWithoutBackPressure();
        }
        subscription.get().request(inflights);
    }

    @Override
    public void cancel() {
        Subscription s = subscription.getAndSet(Subscriptions.CANCELLED);
        if (s != null) {
            s.cancel();
        }
    }

    private void requestNext(Message<?> message) {
        Subscriber<? super Message<?>> down = downstream.get();
        if (down != null) {
            down.onNext(message);
        }
        Subscription up = this.subscription.get();
        if (up != null && inflights != Long.MAX_VALUE) {
            up.request(1);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        Subscriber<? super Message<?>> subscriber = downstream.getAndSet(null);
        if (subscriber != null) {
            subscriber.onError(throwable);
        }
    }

    @Override
    public void onComplete() {
        Subscriber<? super Message<?>> subscriber = downstream.getAndSet(null);
        if (subscriber != null) {
            subscriber.onComplete();
        }
    }
}
