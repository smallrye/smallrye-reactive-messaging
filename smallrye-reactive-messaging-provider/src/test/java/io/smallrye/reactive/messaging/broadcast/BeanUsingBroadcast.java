package io.smallrye.reactive.messaging.broadcast;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.reactivestreams.Publisher;

import io.smallrye.reactive.messaging.annotations.Broadcast;

@ApplicationScoped
public class BeanUsingBroadcast {

    private List<String> l1 = new ArrayList<>();
    private List<String> l2 = new ArrayList<>();

    @Incoming("Y")
    public void y2(String i) {
        l2.add(i);
    }

    @Outgoing("X")
    public Publisher<String> x() {
        return ReactiveStreams.of("a", "b", "c", "d").buildRs();
    }

    @Outgoing("Y")
    @Incoming("X")
    @Broadcast(2)
    public String process(String s) {
        return s.toUpperCase();
    }

    @Incoming("Y")
    public void y1(String i) {
        l1.add(i);
    }

    List<String> l1() {
        return l1;
    }

    List<String> l2() {
        return l2;
    }

}
