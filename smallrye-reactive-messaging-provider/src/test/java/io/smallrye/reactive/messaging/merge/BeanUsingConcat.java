package io.smallrye.reactive.messaging.merge;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Merge;

@ApplicationScoped
public class BeanUsingConcat {

    private List<String> list = new ArrayList<>();

    @Outgoing("X")
    public Flowable<String> x() {
        return Flowable.zip(Flowable.fromArray("a", "b", "c"),
                Flowable.interval(10, TimeUnit.MILLISECONDS),
                (a, b) -> a);
    }

    @Outgoing("Z1")
    public Flowable<String> z1() {
        return Flowable.fromArray("d", "e", "f");
    }

    @Outgoing("X")
    @Incoming("Z2")
    public Flowable<String> y(Flowable<String> z) {
        return z.map(String::toUpperCase);
    }

    @Incoming("X")
    @Merge(Merge.Mode.CONCAT)
    public void sink(String payload) {
        list.add(payload);
    }

    @Outgoing("Z2")
    @Incoming("Z1")
    public Flowable<String> z2(Flowable<String> z) {
        return z
                .zipWith(Flowable.interval(5, TimeUnit.MILLISECONDS), (a, b) -> a)
                .concatWith(Flowable.fromArray("g"));
    }

    public List<String> list() {
        return list;
    }

}
