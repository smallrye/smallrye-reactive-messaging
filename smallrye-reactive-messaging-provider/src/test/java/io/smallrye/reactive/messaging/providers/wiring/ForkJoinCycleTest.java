package io.smallrye.reactive.messaging.providers.wiring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.RepeatedTest;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;

/**
 * Reproduce https://github.com/smallrye/smallrye-reactive-messaging/issues/1229.
 */
public class ForkJoinCycleTest extends WeldTestBaseWithoutTails {

    @RepeatedTest(10)
    public void test() {
        addBeanClass(Hello.class, Fork.class, Join.class, Service2.class, Service1.class);
        Hello hello = installInitializeAndGet(Hello.class);
        Join join = get(Join.class);
        assertThat(hello).isNotNull();
        hello.greeting("test");
        hello.greeting("world");

        await().until(() -> join.list().size() == 4);
    }

    public static class Hello {

        private static final AtomicInteger count = new AtomicInteger(0);

        @Inject
        @Channel("start")
        private Emitter<String> sender;

        public String greeting(String name) {

            String msg = name + count.incrementAndGet();

            sender.send(msg);

            return msg;
        }
    }

    @ApplicationScoped
    public static class Fork {

        @Incoming("start")
        @Outgoing("fork")
        @Broadcast
        public String fork(String str) {
            return "fork " + str;
        }

    }

    @ApplicationScoped
    public static class Join {

        private final List<String> results = new CopyOnWriteArrayList<>();

        @Incoming("join")
        @Merge
        public void join(String s) {
            results.add(s);
        }

        public List<String> list() {
            return results;
        }
    }

    @ApplicationScoped
    public static class Service1 {

        @Incoming("fork")
        @Outgoing("join")
        public String service1(String src) {
            return "service 1 " + src;
        }

    }

    @ApplicationScoped
    public static class Service2 {

        @Incoming("fork")
        @Outgoing("join")
        public String service2(String src) {
            return "service 2 " + src;
        }

    }

}
