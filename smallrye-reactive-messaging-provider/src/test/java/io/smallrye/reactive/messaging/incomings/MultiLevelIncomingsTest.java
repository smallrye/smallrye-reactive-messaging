package io.smallrye.reactive.messaging.incomings;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class MultiLevelIncomingsTest extends WeldTestBaseWithoutTails {

    // sink <- out -- process with multiple incoming <-- a <-- v
    //                                               <-- b
    //                                               <-- c <-- w <-- z

    @Test
    public void test() {
        addBeanClass(A.class, B.class, C.class, V.class, W.class, Z.class, MyProcessor.class, Sink.class);
        initialize();
        Sink sink = container.select(Sink.class).get();
        await().until(() -> sink.list().size() == 8);
    }

    @ApplicationScoped
    public static class Sink {
        private List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MyProcessor {
        @Incoming("a")
        @Incoming("b")
        @Incoming("c")
        @Outgoing("out")
        public Message<String> process(Message<String> s) {
            return Message.of(s.getPayload().toUpperCase());
        }
    }

    @ApplicationScoped
    public static class A {
        @Incoming("v")
        @Outgoing("a")
        public String process(String s) {
            return s + s;
        }
    }

    @ApplicationScoped
    public static class C {
        @Incoming("w")
        @Outgoing("c")
        public String process(String s) {
            return s + s;
        }
    }

    @ApplicationScoped
    public static class B {
        @Outgoing("b")
        public Flowable<String> produce() {
            return Flowable.just("d", "e", "f");
        }
    }

    @ApplicationScoped
    public static class V {
        @Outgoing("v")
        public Flowable<String> produce() {
            return Flowable.just("a", "b", "c");
        }
    }

    @ApplicationScoped
    public static class W {
        @Incoming("z")
        @Outgoing("w")
        public String process(String s) {
            return "_" + s;
        }
    }

    @ApplicationScoped
    public static class Z {
        @Outgoing("z")
        public Flowable<String> produce() {
            return Flowable.just("g", "h");
        }
    }

}
