package io.smallrye.reactive.messaging.pulsar;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class PulsarSource<Message> implements Publisher<Message>, Subscription {

    @Override
    public void subscribe(Subscriber<? super Message> subscriber) {

    }

    @Override
    public void request(long l) {

    }

    @Override
    public void cancel() {

    }
}
