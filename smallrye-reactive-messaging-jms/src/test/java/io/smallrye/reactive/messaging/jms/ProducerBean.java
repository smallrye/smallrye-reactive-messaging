package io.smallrye.reactive.messaging.jms;

import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;

@ApplicationScoped
public class ProducerBean {

    @Outgoing("queue-one")
    public Flowable<Integer> producer() {
        return Flowable.interval(10, TimeUnit.MILLISECONDS)
                .onBackpressureBuffer(10)
                .map(Long::intValue)
                .take(10);
    }

}
