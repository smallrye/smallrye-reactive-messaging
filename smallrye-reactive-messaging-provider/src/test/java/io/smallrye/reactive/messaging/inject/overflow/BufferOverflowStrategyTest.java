package io.smallrye.reactive.messaging.inject.overflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.exceptions.MissingBackpressureException;
import io.reactivex.schedulers.Schedulers;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class BufferOverflowStrategyTest extends WeldTestBaseWithoutTails {

    private static ExecutorService executor;

    @BeforeClass
    public static void init() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterClass
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

        await().until(() -> bean.exception() != null);
        assertThat(bean.output()).doesNotContain("999");
        assertThat(bean.failure()).isNotNull().isInstanceOf(MissingBackpressureException.class);
    }

    @ApplicationScoped
    public static class BeanUsingBufferOverflowStrategy {

        @Inject
        @Channel("hello")
        @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
        Emitter<String> emitter;

        private List<String> output = new CopyOnWriteArrayList<>();

        private volatile Throwable downstreamFailure;
        private Exception callerException;

        public List<String> output() {
            return output;
        }

        public Throwable failure() {
            return downstreamFailure;
        }

        public Exception exception() {
            return callerException;
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
                }
            }).start();
        }

        @Incoming("hello")
        @Outgoing("out")
        public Flowable<String> consume(Flowable<String> values) {
            Scheduler scheduler = Schedulers.from(executor);
            return values
                    .observeOn(scheduler)
                    .delay(1, TimeUnit.MILLISECONDS, scheduler)
                    .doOnError(err -> downstreamFailure = err);
        }

        @Incoming("out")
        public void out(String s) {
            output.add(s);
        }

    }
}
