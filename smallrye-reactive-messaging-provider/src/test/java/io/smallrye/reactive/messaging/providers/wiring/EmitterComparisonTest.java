package io.smallrye.reactive.messaging.providers.wiring;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.assertj.core.api.Assertions;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.RepeatedTest;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;

/**
 * Reproducer for https://github.com/smallrye/smallrye-reactive-messaging/issues/1600
 */
public class EmitterComparisonTest extends WeldTestBaseWithoutTails {

    @RepeatedTest(10)
    public void test() {
        List<Class<?>> classes = Arrays.asList(Sink.class, MyApp.class, MyApp1.class, MyApp2.class, MyApp3.class, MyApp4.class,
                MyApp5.class,
                MyApp6.class, MyApp7.class, MyApp8.class, MyApp9.class, MyApp10.class, MyApp11.class, MyApp12.class,
                MyApp13.class, MyApp14.class, MyApp15.class);
        Collections.shuffle(classes);
        addBeanClass(classes.toArray(new Class[0]));
        Assertions.assertThat(installInitializeAndGet(MyApp.class)).isNotNull();
    }

    @ApplicationScoped
    public static class Sink {
        @Incoming("foo")
        @Incoming("bar")
        @Merge
        public void consume(String s) {
            // do nothing
        }
    }

    @ApplicationScoped
    public static class MyApp {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp1 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp2 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp3 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp4 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp5 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp6 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp7 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp8 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp9 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp10 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp11 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp12 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp13 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp14 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }

    @ApplicationScoped
    public static class MyApp15 {
        @Outgoing("foo")
        public Multi<String> foo() {
            return Multi.createFrom().empty();
        }

        @Inject
        @Channel("bar")
        @Broadcast
        Emitter<String> bar;
    }
}
