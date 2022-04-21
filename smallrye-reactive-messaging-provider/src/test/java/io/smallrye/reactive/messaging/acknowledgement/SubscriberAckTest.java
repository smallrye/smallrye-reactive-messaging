package io.smallrye.reactive.messaging.acknowledgement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.providers.ProcessingException;
import io.vertx.core.impl.ConcurrentHashSet;

public class SubscriberAckTest extends WeldTestBaseWithoutTails {

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulConsumptionOfPayload() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(SuccessfulPayloadConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        SuccessfulPayloadConsumer consumer = container.getBeanManager().createInstance().select(
                SuccessfulPayloadConsumer.class).get();
        run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingConsumptionOfPayload() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(FailingPayloadConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        FailingPayloadConsumer consumer = container.getBeanManager().createInstance().select(
                FailingPayloadConsumer.class).get();
        List<Throwable> reasons = run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 8);
        assertThat(acked).hasSize(8);
        assertThat(nacked).hasSize(2);
        assertThat(reasons).hasSize(2);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulBlockingConsumptionOfPayload() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(BlockingSuccessfulPayloadConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        BlockingSuccessfulPayloadConsumer consumer = container.getBeanManager().createInstance().select(
                BlockingSuccessfulPayloadConsumer.class).get();
        run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingBlockingConsumptionOfPayload() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(BlockingFailingPayloadConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        BlockingFailingPayloadConsumer consumer = container.getBeanManager().createInstance().select(
                BlockingFailingPayloadConsumer.class).get();
        List<Throwable> reasons = run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 8);
        await().until(() -> nacked.size() == 2);
        await().until(() -> reasons.size() == 2);
        assertThat(acked).hasSize(8);
        assertThat(nacked).hasSize(2);
        assertThat(reasons).hasSize(2);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulAsyncConsumptionOfPayload() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(SuccessfulPayloadAsyncConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        SuccessfulPayloadAsyncConsumer consumer = container.getBeanManager().createInstance().select(
                SuccessfulPayloadAsyncConsumer.class).get();
        run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingAsyncConsumptionOfPayload() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(FailingPayloadAsyncConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        FailingPayloadAsyncConsumer consumer = container.getBeanManager().createInstance().select(
                FailingPayloadAsyncConsumer.class).get();
        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(8);
        assertThat(nacked).hasSize(2);
        assertThat(throwables).hasSize(2)
                .anySatisfy(t -> assertThat(t).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("2"))
                .anySatisfy(t -> assertThat(t)
                        .isInstanceOf(ProcessingException.class)
                        .hasCauseInstanceOf(InvocationTargetException.class).hasStackTraceContaining("8"));
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulAsyncConsumptionOfMessage() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(SuccessfulMessageAsyncConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        SuccessfulMessageAsyncConsumer consumer = container.getBeanManager().createInstance().select(
                SuccessfulMessageAsyncConsumer.class).get();
        run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingAsyncConsumptionOfMessage() throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(FailingMessageAsyncConsumer.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        FailingMessageAsyncConsumer consumer = container.getBeanManager().createInstance().select(
                FailingMessageAsyncConsumer.class).get();
        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(8);
        assertThat(nacked).hasSize(2);
        assertThat(throwables).hasSize(2)
                .anySatisfy(t -> assertThat(t).isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("2"))
                .anySatisfy(t -> assertThat(t)
                        .isInstanceOf(ProcessingException.class)
                        .hasCauseInstanceOf(InvocationTargetException.class).hasStackTraceContaining("8"));
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulConsumptionOfPayloadUsingSubscriber()
            throws InterruptedException {
        addBeanClass(EmitterBean.class);
        addBeanClass(SuccessfulPayloadSubscriber.class);
        initialize();

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        Emitter<String> emitter = container.getBeanManager().createInstance().select(EmitterBean.class).get().emitter();
        SuccessfulPayloadSubscriber consumer = container.getBeanManager().createInstance().select(
                SuccessfulPayloadSubscriber.class).get();
        run(acked, nacked, emitter);

        await().until(() -> consumer.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    private List<Throwable> run(Set<String> acked, Set<String> nacked, Emitter<String> emitter)
            throws InterruptedException {
        List<Throwable> reasons = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        Multi.createFrom().range(0, 10)
                .onItem().transform(i -> Integer.toString(i))
                .onItem()
                .transformToUniAndMerge(i -> Uni.createFrom()
                        .completionStage(CompletableFuture.runAsync(() -> emitter.send(Message.of(i, Metadata.empty(),
                                () -> {
                                    acked.add(i);
                                    return CompletableFuture.completedFuture(null);
                                }, t -> {
                                    reasons.add(t);
                                    nacked.add(i);
                                    return CompletableFuture.completedFuture(null);
                                })))
                                .thenApply(x -> i)))
                .subscribe().with(
                        x -> {
                            // noop
                        },
                        f -> {
                            // noop
                        },
                        done::countDown);

        done.await(10, TimeUnit.SECONDS);
        return reasons;
    }

    @ApplicationScoped
    public static class SuccessfulPayloadConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class FailingPayloadConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public void consume(String s) {
            if (s.equalsIgnoreCase("2") || s.equalsIgnoreCase("8")) {
                throw new IllegalArgumentException("boom");
            }
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class BlockingSuccessfulPayloadConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(String s) {
            CompletableFuture.runAsync(() -> list.add(s)).join();
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class BlockingFailingPayloadConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Blocking
        public void consume(String s) {
            CompletableFuture.runAsync(() -> {
                if (s.equalsIgnoreCase("2") || s.equalsIgnoreCase("8")) {
                    throw new IllegalArgumentException("boom");
                }
                list.add(s);
            }).join();
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class SuccessfulPayloadAsyncConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(String s) {
            list.add(s);
            return CompletableFuture.completedFuture(null);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class FailingPayloadAsyncConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(String s) {
            list.add(s);
            if (s.equalsIgnoreCase("2")) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(new IllegalArgumentException("2"));
                return future;
            } else if (s.equalsIgnoreCase("8")) {
                throw new IllegalStateException("8");
            }
            return CompletableFuture.completedFuture(null);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class SuccessfulMessageAsyncConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public CompletionStage<Void> consume(Message<String> s) {
            list.add(s.getPayload());
            return s.ack();
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class FailingMessageAsyncConsumer {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        @Acknowledgment(Acknowledgment.Strategy.POST_PROCESSING)
        public CompletionStage<Void> consume(Message<String> s) {
            list.add(s.getPayload());
            if (s.getPayload().equalsIgnoreCase("2")) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(new IllegalArgumentException("2"));
                return future;
            } else if (s.getPayload().equalsIgnoreCase("8")) {
                throw new IllegalStateException("8");
            }
            return CompletableFuture.completedFuture(null);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class SuccessfulPayloadSubscriber {

        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("data")
        public Subscriber<String> consume() {
            return ReactiveStreams.<String> builder()
                    .forEach(s -> list.add(s))
                    .build();
        }

        public List<String> list() {
            return list;
        }

    }

}
