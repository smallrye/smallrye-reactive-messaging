package io.smallrye.reactive.messaging.wiring;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.RepeatedTest;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

import static org.assertj.core.api.Assertions.assertThat;

public class EmitterMethodCycleTest extends WeldTestBaseWithoutTails {

    @RepeatedTest(10)
    public void test() {
        addBeanClass(Sink.class, MyApp.class);
        assertThat(installInitializeAndGet(MyApp.class)).isNotNull();
    }

    @ApplicationScoped
    public static class Sink {
        @Incoming("foo")
        @Incoming("bar")
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
        Emitter<String> bar;
    }

}
