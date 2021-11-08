package io.smallrye.reactive.messaging.providers.helpers;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class IgnoringSubscriber implements Subscriber<Message<?>> {

    public static final Subscriber<Message<?>> INSTANCE = new IgnoringSubscriber();

    private IgnoringSubscriber() {
        // Avoid direct instantiation
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onError(Throwable throwable) {
        // Ignored.
    }

    @Override
    public void onComplete() {
        // Ignored
    }

    @Override
    public void onNext(Message<?> message) {
        // Ignored
    }
}
