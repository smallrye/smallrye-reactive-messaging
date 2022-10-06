package io.smallrye.reactive.messaging.inject.overflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.OnOverflow;

public class LegacyEmitterBufferOverflowStrategyTest extends WeldTestBaseWithoutTails {

    private static ExecutorService executor;

    @BeforeAll
    public static void init() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterAll
    public static void cleanup() {
        executor.shutdown();
    }

    @Test
    public void testNormal() {
        BeanUsingBufferOverflowStrategy bean = installInitializeAndGet(BeanUsingBufferOverflowStrategy.class);
        bean.emitThree();

        await().until(() -> bean.output().size() == 3);
        assertThat(bean.output()).containsExactly("1", "2", "3");
        assertThat(bean.exception()).isNull();
    }

    @Test
    public void testOverflow() {
        BeanUsingBufferOverflowStrategy bean = installInitializeAndGet(BeanUsingBufferOverflowStrategy.class);
        bean.emitALotOfItems();

        await().until(bean::isComplete);
        assertThat(bean.output()).doesNotContain("999");
        assertThat(bean.exception()).isInstanceOf(IllegalStateException.class);
    }

    @SuppressWarnings("deprecation")
    @ApplicationScoped
    public static class BeanUsingBufferOverflowStrategy {

        @Inject
        @Channel("hello")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
        Emitter<String> emitter;

        private final List<String> output = new CopyOnWriteArrayList<>();

        private Exception callerException;
        private final AtomicBoolean completed = new AtomicBoolean();

        public List<String> output() {
            return output;
        }

        public Exception exception() {
            return callerException;
        }

        public boolean isComplete() {
            return completed.get();
        }

        public void emitThree() {
            try {
                emitter.send("1");
                emitter.send("2");
                emitter.send("3");
                emitter.complete();

            } catch (Exception e) {
                callerException = e;
            }
        }

        public void emitALotOfItems() {
            new Thread(() -> {
                try {
                    for (int i = 1; i < 1000; i++) {
                        emitter.send("" + i);
                    }
                } catch (Exception e) {
                    callerException = e;
                } finally {
                    completed.set(true);
                }
            }).start();
        }

        @Incoming("hello")
        @Outgoing("out")
        public Uni<String> consume(String value) {
            if (completed.get()) {
                return Uni.createFrom().nullItem();
            } else {
                return Uni.createFrom().item(value)
                        .onItem().delayIt().by(Duration.ofMillis(2));
            }
        }

        @Incoming("out")
        public void out(String s) {
            output.add(s);
        }

    }
}
