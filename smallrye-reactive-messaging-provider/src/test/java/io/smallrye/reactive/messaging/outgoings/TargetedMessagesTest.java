package io.smallrye.reactive.messaging.outgoings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.Targeted;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;

public class TargetedMessagesTest extends WeldTestBaseWithoutTails {

    @AfterEach
    public void cleanup() {
        System.clearProperty(MediatorManager.STRICT_MODE_PROPERTY);
    }

    @Test
    public void testOutgoingsWithThreeSources() {
        addBeanClass(IncomingOnA.class, IncomingOnB.class, IncomingOnC.class);
        addBeanClass(MultipleOutgoingsProducer.class);

        initialize();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();
        IncomingOnC inC = container.select(IncomingOnC.class).get();

        await().until(() -> inA.list().size() == 3);
        assertThat(inA.list()).contains("a-b", "a-c", "a-d");
        await().until(() -> inB.list().size() == 3);
        assertThat(inB.list()).contains("b-a", "b-c", "b-d");
        await().until(() -> inC.list().size() == 3);
        assertThat(inC.list()).contains("c-a", "c-b", "c-d");
    }

    @Test
    public void testCombiningOutgoingsAndBroadcastWithAProcessor() {
        addBeanClass(MySource.class);
        addBeanClass(IncomingOnA.class);
        addBeanClass(ProcessorUsingMultipleOutgoingsAndBroadcast.class);
        addBeanClass(IncomingOnB.class);

        initialize();
        ProcessorUsingMultipleOutgoingsAndBroadcast bean = container
                .select(ProcessorUsingMultipleOutgoingsAndBroadcast.class).get();
        MySource source = container.select(MySource.class).get();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();

        await().until(() -> bean.list().size() == 6);
        await().until(() -> inA.list().size() == 6);
        await().until(() -> inB.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
        assertThat(inA.list()).contains("a-A", "a-B", "a-C", "a-D", "a-E", "a-F");
        assertThat(inB.list()).contains("b-A", "b-B", "b-C", "b-D", "b-E", "b-F");
        assertThat(source.acked()).contains("a", "b", "c", "d", "e", "f");
    }

    @ApplicationScoped
    public static class IncomingOnA {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("a")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class IncomingOnB {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("b")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class IncomingOnC {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("c")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class MultipleOutgoingsProducer {

        @Outgoing("a")
        @Outgoing("b")
        @Outgoing("c")
        public Publisher<Targeted> produce() {
            return Flowable.just(
                    Targeted.of("b", "b-a", "c", "c-a"),
                    Targeted.of("a", "a-b", "c", "c-b"),
                    Targeted.of("a", "a-c", "b", "b-c"),
                    Targeted.of("a", "a-d", "b", "b-d", "c", "c-d"));
        }

    }

    @ApplicationScoped
    public static class ProcessorUsingMultipleOutgoingsAndBroadcast {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        @Outgoing("a")
        @Outgoing("b")
        public Targeted consume(String s) {
            list.add(s);
            return Targeted.of("a", "a-" + s.toUpperCase(),
                    "b", "b-" + s.toUpperCase());
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MySource {

        List<String> acked = new CopyOnWriteArrayList<>();

        @Outgoing("in")
        public Multi<Message<String>> produce() {
            return Multi.createFrom().items(
                    Message.of("a", () -> CompletableFuture.completedFuture(null).thenAccept(x -> acked.add("a"))),
                    Message.of("b", () -> CompletableFuture.completedFuture(null).thenAccept(x -> acked.add("b"))),
                    Message.of("c", () -> CompletableFuture.completedFuture(null).thenAccept(x -> acked.add("c"))),
                    Message.of("d", () -> CompletableFuture.completedFuture(null).thenAccept(x -> acked.add("d"))),
                    Message.of("e", () -> CompletableFuture.completedFuture(null).thenAccept(x -> acked.add("e"))),
                    Message.of("f", () -> CompletableFuture.completedFuture(null).thenAccept(x -> acked.add("f"))));
        }

        public List<String> acked() {
            return acked;
        }
    }

}
