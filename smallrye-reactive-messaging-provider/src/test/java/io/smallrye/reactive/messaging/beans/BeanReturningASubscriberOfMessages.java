package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Subscriber;

@ApplicationScoped
public class BeanReturningASubscriberOfMessages {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    Flow.Subscriber<Message<String>> create() {
        return new Flow.Subscriber<>() {

            Flow.Subscription s;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                s = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(Message<String> item) {
                list.add(item.getPayload());
                s.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                //ignore
            }

            @Override
            public void onComplete() {
                //ignore
            }
        };
    }

    public List<String> payloads() {
        return list;
    }

}
