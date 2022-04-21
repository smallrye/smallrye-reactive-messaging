package io.smallrye.reactive.messaging.inject.overflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class ThrowExceptionOverflowStrategyTest extends WeldTestBaseWithoutTails {

    private static ExecutorService executor;

    @BeforeAll
    public static void init() {
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterAll
    public static void cleanup() {
        executor.shutdown();
    }

    @Test
    public void testNormal() throws InterruptedException {
        BeanUsingThrowExceptionOverflowStrategy bean = installInitializeAndGet(
                BeanUsingThrowExceptionOverflowStrategy.class);
        bean.emitOne();

        await().until(() -> bean.output().size() == 1);
        assertThat(bean.output()).containsExactly("1");
        assertThat(bean.accepted()).containsExactly("1");
        assertThat(bean.rejected()).isEmpty();
    }

    @Test
    public void testOverflow() throws InterruptedException, ExecutionException, TimeoutException {
        BeanUsingThrowExceptionOverflowStrategy bean = installInitializeAndGet(
                BeanUsingThrowExceptionOverflowStrategy.class);
        Future<?> future = bean.emit999();

        future.get(10, TimeUnit.SECONDS); // Will throw exception if emitter.send() throws an unexpected exception

        assertThat(bean.accepted().size() + bean.rejected().size()).isEqualTo(999);
        assertThat(bean.accepted()).contains("1");
        assertThat(bean.rejected()).isNotEmpty();

        await().until(() -> bean.output().size() == bean.accepted().size());
        assertThat(bean.output()).containsExactlyElementsOf(bean.accepted());
        assertThat(bean.failure()).isNull();
    }

    @ApplicationScoped
    public static class BeanUsingThrowExceptionOverflowStrategy {

        @Inject
        @Channel("hello")
        @OnOverflow(value = OnOverflow.Strategy.THROW_EXCEPTION)
        Emitter<String> emitter;

        private List<String> output = new CopyOnWriteArrayList<>();
        private List<String> accepted = new CopyOnWriteArrayList<>();
        private List<String> rejected = new CopyOnWriteArrayList<>();

        private volatile Throwable downstreamFailure;

        public List<String> output() {
            return output;
        }

        public List<String> accepted() {
            return accepted;
        }

        public List<String> rejected() {
            return rejected;
        }

        public Throwable failure() {
            return downstreamFailure;
        }

        public void emitOne() throws InterruptedException {
            emit("1");
            emitter.complete();
        }

        public Future<?> emit999() {
            return executor.submit(() -> {
                for (int i = 1; i < 1000; i++) {
                    emit("" + i);
                }
                return null;
            });
        }

        private void emit(String item) throws InterruptedException {
            try {
                emitter.send(item);
                accepted.add(item);
            } catch (IllegalStateException e) {
                rejected.add(item);
                Thread.sleep(1);
            }
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
