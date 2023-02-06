package io.smallrye.reactive.messaging.merge;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.annotations.Merge;

@ApplicationScoped
public class BeanUsingOne {

    private final List<String> list = new ArrayList<>();

    @Outgoing("X")
    public Multi<String> x() {
        return Multi.createBy().combining().streams(
                Multi.createFrom().items("a", "b", "c"),
                Multi.createFrom().ticks().every(Duration.ofMillis(10)))
                .asTuple()
                .map(Tuple2::getItem1);
    }

    @Outgoing("Z1")
    public Multi<String> z1() {
        return Multi.createFrom().items("d", "e", "f");
    }

    @Outgoing("X")
    @Incoming("Z2")
    public Multi<String> y(Multi<String> z) {
        return z.map(String::toUpperCase);
    }

    @Incoming("X")
    @Merge(Merge.Mode.ONE)
    public void sink(String payload) {
        list.add(payload);
    }

    @Outgoing("Z2")
    @Incoming("Z1")
    public Multi<String> z2(Multi<String> z) {
        return Multi.createBy().combining().streams(
                z,
                Multi.createFrom().ticks().every(Duration.ofMillis(5)))
                .asTuple()
                .map(Tuple2::getItem1);
    }

    public List<String> list() {
        return list;
    }

}
