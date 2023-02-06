package io.smallrye.reactive.messaging;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.wiring.Wiring;

public class SubscriptionCancellationTest extends WeldTestBaseWithoutTails {

    @Test
    public void testThatTerminationCancelTheStreamWithAnExplicitCall() {
        addBeanClass(MyApp.class);
        MyApp app = installInitializeAndGet(MyApp.class);

        Wiring wiring = get(Wiring.class);
        wiring.terminateAllComponents();

        await().until(app::isBCancelled);
        await().until(app::isACancelled);
    }

    @Test
    public void testThatTerminationCancelTheStreamWhenContainerIsShutdown() {
        addBeanClass(MyApp.class);
        initialize();

        container.close();

        await().until(MyApp.cancelled_b::get);
        await().until(MyApp.cancelled_a::get);
    }

    @Test
    public void testThatEmitterTerminationCancelTheStreamWhenContainerIsShutdown() {
        addBeanClass(MyAppUsingEmitter.class);
        installInitializeAndGet(MyAppUsingEmitter.class).generate();

        container.close();

        await().until(MyAppUsingEmitter.cancelled_b::get);
        await().until(MyAppUsingEmitter.cancelled_a::get);
    }

    @ApplicationScoped
    public static class MyApp {

        private static final AtomicBoolean cancelled_a = new AtomicBoolean();
        private static final AtomicBoolean cancelled_b = new AtomicBoolean();

        @Outgoing("a")
        public Multi<Long> generate() {
            return Multi.createFrom().ticks().every(Duration.ofMillis(10))
                    .onCancellation().invoke(() -> cancelled_a.set(true));
        }

        @Incoming("a")
        @Outgoing("b")
        public Multi<String> process(Multi<Long> multi) {
            return multi
                    .map(l -> Long.toString(l))
                    .onCancellation().invoke(() -> cancelled_b.set(true));
        }

        @Incoming("b")
        public void consume(String s) {
            // do nothing
        }

        public boolean isACancelled() {
            return cancelled_a.get();
        }

        public boolean isBCancelled() {
            return cancelled_b.get();
        }

    }

    @ApplicationScoped
    public static class MyAppUsingEmitter {

        private static final AtomicBoolean cancelled_a = new AtomicBoolean();
        private static final AtomicBoolean cancelled_b = new AtomicBoolean();
        private final Emitter<Long> emitter;

        @Inject
        public MyAppUsingEmitter(@Channel("a") Emitter<Long> emitter) {
            this.emitter = emitter;
        }

        public void generate() {
            new Thread(() -> {
                while (!emitter.isCancelled()) {
                    try {
                        emitter.send(1L);
                    } catch (IllegalStateException e) {
                        // Cancellation may happen concurrently,
                        // ignore and continue
                        break;
                    }
                }
                cancelled_a.set(true);
            }).start();
        }

        @Incoming("a")
        @Outgoing("b")
        public Multi<String> process(Multi<Long> multi) {
            return multi
                    .map(l -> Long.toString(l))
                    .onCancellation().invoke(() -> cancelled_b.set(true));
        }

        @Incoming("b")
        public void consume(String s) {
            // do nothing
        }

    }
}
