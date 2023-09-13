package io.smallrye.reactive.messaging.outgoings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.multi.split.MultiSplitter;
import io.smallrye.reactive.messaging.Targeted;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class MultiLevelOutgoingsTest extends WeldTestBaseWithoutTails {

    // source --> -- first processor -- a --> x
    //                                  b
    //                                  c --> -- second processor -- --> y
    //                                                               --> z -- branch processor -- --> w
    //                                                                                            --> v

    @Test
    public void test() {
        addBeanClass(A.class, B.class, X.class, Y.class, W.class, V.class, FirstProcessor.class,
                SecondProcessor.class, Source.class, SplitBranchProcessor.class);
        initialize();
        B b = container.select(B.class).get();
        X x = container.select(X.class).get();
        Y y = container.select(Y.class).get();
        W w = container.select(W.class).get();
        V v = container.select(V.class).get();
        await().until(() -> b.list().size() == 6);
        await().untilAsserted(() -> assertThat(b.list()).containsExactly("A", "B", "C", "D", "E", "F"));
        await().until(() -> x.list().size() == 6);
        await().untilAsserted(() -> assertThat(x.list()).containsExactly("_A", "_B", "_C", "_D", "_E", "_F"));
        await().until(() -> y.list().size() == 3);
        await().untilAsserted(() -> assertThat(y.list()).containsExactly("B", "D", "F"));
        await().until(() -> w.list().size() == 1);
        await().untilAsserted(() -> assertThat(w.list()).containsExactly("C"));
        await().until(() -> v.list().size() == 2);
        await().untilAsserted(() -> assertThat(v.list()).containsExactly("A", "E"));
    }

    @ApplicationScoped
    public static class Source {
        @Outgoing("source")
        public Multi<String> produce() {
            return Multi.createFrom().items("a", "b", "c", "d", "e", "f");
        }
    }

    @ApplicationScoped
    public static class FirstProcessor {
        @Incoming("source")
        @Outgoing("a")
        @Outgoing("b")
        @Outgoing("c")
        public Message<String> process(Message<String> s) {
            return Message.of(s.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class SecondProcessor {

        AtomicInteger i = new AtomicInteger();

        @Incoming("c")
        @Outgoing("y")
        @Outgoing("z")
        public Multi<Targeted> process(Multi<String> multi) {
            return multi.map(s -> i.incrementAndGet() % 2 == 0 ? Targeted.of("y", s) : Targeted.of("z", s));
        }
    }

    public static class SplitBranchProcessor {

        AtomicInteger i = new AtomicInteger();

        public enum FLIPFLOP {
            W,
            V
        }

        @Incoming("z")
        @Outgoing("v")
        @Outgoing("w")
        public MultiSplitter<String, FLIPFLOP> process(Multi<String> multi) {
            return multi.split(FLIPFLOP.class, s -> i.incrementAndGet() % 2 == 0 ? FLIPFLOP.W : FLIPFLOP.V);
        }
    }

    @ApplicationScoped
    public static class A {
        @Incoming("a")
        @Outgoing("x")
        public String process(String s) {
            return "_" + s;
        }
    }

    @ApplicationScoped
    public static class B {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("b")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class X {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("x")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class Y {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("y")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class W {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("w")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class V {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("v")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

}
