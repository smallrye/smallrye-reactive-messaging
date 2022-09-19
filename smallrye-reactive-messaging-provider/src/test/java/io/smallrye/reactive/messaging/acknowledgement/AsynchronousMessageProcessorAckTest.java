package io.smallrye.reactive.messaging.acknowledgement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;

public class AsynchronousMessageProcessorAckTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    public void deployBase() {
        addBeanClass(EmitterBean.class, Sink.class);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfMessage() throws InterruptedException {
        addBeanClass(SuccessfulMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = ConcurrentHashMap.newKeySet();
        Set<String> nacked = ConcurrentHashMap.newKeySet();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfMessageUni() throws InterruptedException {
        addBeanClass(SuccessfulMessageProcessorUni.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = ConcurrentHashMap.newKeySet();
        Set<String> nacked = ConcurrentHashMap.newKeySet();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingProcessingOfMessage() throws InterruptedException {
        addBeanClass(FailingMessageProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = ConcurrentHashMap.newKeySet();
        Set<String> nacked = ConcurrentHashMap.newKeySet();

        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 8);
        assertThat(acked).hasSize(9);
        assertThat(nacked).hasSize(1);
        assertThat(throwables).hasSize(1);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingProcessingOfMessageUni() throws InterruptedException {
        addBeanClass(FailingMessageProcessorUni.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = ConcurrentHashMap.newKeySet();
        Set<String> nacked = ConcurrentHashMap.newKeySet();

        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 8);
        assertThat(acked).hasSize(9);
        assertThat(nacked).hasSize(1);
        assertThat(throwables).hasSize(1);
    }

    private List<Throwable> run(Set<String> acked, Set<String> nacked, Emitter<String> emitter)
            throws InterruptedException {
        List<Throwable> reasons = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        Multi.createFrom().items("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
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
    public static class Sink {
        List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class SuccessfulMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        public CompletionStage<Message<String>> process(Message<String> m) {
            String s = m.getPayload();
            return CompletableFuture.supplyAsync(() -> m.withPayload(s.toUpperCase()));
        }

    }

    @ApplicationScoped
    public static class SuccessfulMessageProcessorUni {

        @Incoming("data")
        @Outgoing("out")
        public Uni<Message<String>> process(Message<String> m) {
            String s = m.getPayload();
            return Uni.createFrom()
                    .completionStage(CompletableFuture.supplyAsync(() -> m.withPayload(s.toUpperCase())));
        }

    }

    @ApplicationScoped
    public static class FailingMessageProcessor {

        @Incoming("data")
        @Outgoing("out")
        public CompletionStage<Message<String>> process(Message<String> m) {
            String s = m.getPayload();
            if (s.equalsIgnoreCase("b")) {
                return m.nack(new IllegalArgumentException("b")).thenApply(x -> null);
            }

            if (s.equalsIgnoreCase("e")) {
                // acked by not forwarded
                return m.ack().thenApply(x -> null);
            }

            return CompletableFuture.supplyAsync(() -> m.withPayload(s.toUpperCase()));
        }

    }

    @ApplicationScoped
    public static class FailingMessageProcessorUni {

        @Incoming("data")
        @Outgoing("out")
        public Uni<Message<String>> process(Message<String> m) {
            String s = m.getPayload();
            if (s.equalsIgnoreCase("b")) {
                return Uni.createFrom().completionStage(m.nack(new IllegalArgumentException("b")))
                        .onItem().ignore().andSwitchTo(Uni.createFrom().nullItem());
            }

            if (s.equalsIgnoreCase("e")) {
                return Uni.createFrom().completionStage(m.ack())
                        .onItem().ignore().andSwitchTo(Uni.createFrom().nullItem());
            }

            return Uni.createFrom()
                    .completionStage(CompletableFuture.supplyAsync(() -> m.withPayload(s.toUpperCase())));
        }

    }
}
