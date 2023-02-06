package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class BeanReturningASubscriberOfPayloads {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    public Flow.Subscriber<String> create() {
        return new Flow.Subscriber<>() {
            Flow.Subscription s;

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                s = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(String item) {
                list.add(item);
                s.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                // ignore
            }

            @Override
            public void onComplete() {
                // ignore
            }
        };
    }

    public List<String> payloads() {
        return list;
    }

}
