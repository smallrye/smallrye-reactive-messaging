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
import io.vertx.core.impl.ConcurrentHashSet;

public class AsynchronousPayloadProcessorAckTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    public void deployBase() {
        addBeanClass(EmitterBean.class, Sink.class);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfPayload() throws InterruptedException {
        addBeanClass(SuccessfulPayloadProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfPayloadReturningCompletionStageOfMessage()
            throws InterruptedException {
        addBeanClass(SuccessfulPayloadProcessorCompletionStageOfMessage.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfPayloadReturningUniOfMessage() throws InterruptedException {
        addBeanClass(SuccessfulPayloadProcessorUniOfMessage.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreAckedAfterSuccessfulProcessingOfPayloadUni() throws InterruptedException {
        addBeanClass(SuccessfulPayloadProcessorUni.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 10);
        assertThat(acked).hasSize(10);
        assertThat(nacked).hasSize(0);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingProcessingOfPayload() throws InterruptedException {
        addBeanClass(FailingPayloadProcessor.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 7);
        assertThat(acked).hasSize(8);
        assertThat(nacked).hasSize(2);
        assertThat(throwables).hasSize(2);
    }

    @Test
    public void testThatMessagesAreNackedAfterFailingProcessingOfPayloadUni() throws InterruptedException {
        addBeanClass(FailingPayloadProcessorUni.class);
        initialize();
        Emitter<String> emitter = get(EmitterBean.class).emitter();
        Sink sink = get(Sink.class);

        Set<String> acked = new ConcurrentHashSet<>();
        Set<String> nacked = new ConcurrentHashSet<>();

        List<Throwable> throwables = run(acked, nacked, emitter);

        await().until(() -> sink.list().size() == 7);
        assertThat(acked).hasSize(8);
        assertThat(nacked).hasSize(2);
        assertThat(throwables).hasSize(2);
    }

    private List<Throwable> run(Set<String> acked, Set<String> nacked, Emitter<String> emitter)
            throws InterruptedException {
        List<Throwable> reasons = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);
        Multi.createFrom().items("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
                .onItem()
                .transformToUniAndMerge(i -> Uni.createFrom().completionStage(
                        CompletableFuture.runAsync(() -> emitter.send(Message.of(i, Metadata.empty(),
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
    public static class SuccessfulPayloadProcessor {

        @Incoming("data")
        @Outgoing("out")
        public CompletionStage<String> process(String s) {
            return CompletableFuture.supplyAsync(s::toUpperCase);
        }

    }

    @ApplicationScoped
    public static class SuccessfulPayloadProcessorUni {

        @Incoming("data")
        @Outgoing("out")
        public Uni<String> process(String s) {
            return Uni.createFrom().completionStage(CompletableFuture.supplyAsync(s::toUpperCase));
        }

    }

    @ApplicationScoped
    public static class SuccessfulPayloadProcessorCompletionStageOfMessage {

        @Incoming("data")
        @Outgoing("out")
        public CompletionStage<Message<String>> process(String s) {
            return CompletableFuture.supplyAsync(() -> Message.of(s.toUpperCase()));
        }

    }

    @ApplicationScoped
    public static class SuccessfulPayloadProcessorUniOfMessage {

        @Incoming("data")
        @Outgoing("out")
        public Uni<Message<String>> process(String s) {
            return Uni.createFrom().completionStage(CompletableFuture.supplyAsync(() -> Message.of(s.toUpperCase())));
        }

    }

    @ApplicationScoped
    public static class FailingPayloadProcessor {

        @Incoming("data")
        @Outgoing("out")
        public CompletionStage<String> process(String s) {
            if (s.equalsIgnoreCase("b")) {
                // nacked
                throw new IllegalArgumentException("b");
            }

            if (s.equalsIgnoreCase("e")) {
                // acked by not forwarded
                return CompletableFuture.completedFuture(null);
            }

            if (s.equalsIgnoreCase("f")) {
                // nacked - must not return `null`
                return null;
            }

            return CompletableFuture.completedFuture(s.toUpperCase());
        }

    }

    @ApplicationScoped
    public static class FailingPayloadProcessorUni {

        @Incoming("data")
        @Outgoing("out")
        public Uni<String> process(String s) {
            if (s.equalsIgnoreCase("b")) {
                // nacked.
                throw new IllegalArgumentException("b");
            }

            if (s.equalsIgnoreCase("e")) {
                // acked by not forwarded
                return Uni.createFrom().nullItem();
            }

            if (s.equalsIgnoreCase("f")) {
                // nacked - must not return `null`
                return null;
            }

            return Uni.createFrom().item(s.toUpperCase());
        }

    }
}
