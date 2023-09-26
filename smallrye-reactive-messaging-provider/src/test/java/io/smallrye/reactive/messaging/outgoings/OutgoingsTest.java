package io.smallrye.reactive.messaging.outgoings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.annotations.Outgoings;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;

public class OutgoingsTest extends WeldTestBaseWithoutTails {

    @AfterEach
    public void cleanup() {
        System.clearProperty(MediatorManager.STRICT_MODE_PROPERTY);
    }

    @Test
    public void testInvalidOutgoings() {
        addBeanClass(IncomingOnA.class);
        addBeanClass(InvalidOutgoings.class);

        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);

    }

    @Test
    public void testEmptyOutgoings() {
        addBeanClass(IncomingOnA.class);
        addBeanClass(EmptyOutgoings.class);

        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testInvalidBeanWithOutgoings() {
        addBeanClass(IncomingOnA.class);
        addBeanClass(InvalidBeanWithOutgoings.class);

        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testOutgoingsWithThreeSources() {
        addBeanClass(IncomingOnA.class, IncomingOnB.class, IncomingOnC.class);
        addBeanClass(MultipleOutgoingsProducer.class);

        initialize();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();
        IncomingOnC inC = container.select(IncomingOnC.class).get();

        await().until(() -> inA.list().size() == 6);
        assertThat(inA.list()).contains("a", "b", "c", "d", "e", "f");
        await().until(() -> inB.list().size() == 6);
        assertThat(inB.list()).contains("a", "b", "c", "d", "e", "f");
        await().until(() -> inC.list().size() == 6);
        assertThat(inC.list()).contains("a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testOutgoings() {
        addBeanClass(IncomingOnA.class, IncomingOnB.class);
        addBeanClass(MyBeanUsingOutgoings.class);

        initialize();
        MyBeanUsingOutgoings bean = container.select(MyBeanUsingOutgoings.class).get();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();

        await().until(() -> inA.list().size() == 6);
        assertThat(inA.list()).contains("a", "b", "c", "d", "e", "f");
        await().until(() -> inB.list().size() == 6);
        assertThat(inB.list()).contains("a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testOutgoingsWithMissingDownstream() {
        addBeanClass(IncomingOnC.class);
        addBeanClass(MultipleOutgoingsProducer.class);
        initialize();
    }

    @Test
    public void testOutgoingsWithMissingDownstreamInStrictMode() {
        System.setProperty(MediatorManager.STRICT_MODE_PROPERTY, "true");
        addBeanClass(IncomingOnB.class);
        addBeanClass(MultipleOutgoingsProducer.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testProcessorUsingMultipleOutgoings() {
        addBeanClass(MySource.class);
        addBeanClass(IncomingOnA.class);
        addBeanClass(ProcessorUsingMultipleOutgoings.class);
        addBeanClass(IncomingOnB.class);

        initialize();
        ProcessorUsingMultipleOutgoings bean = container.select(ProcessorUsingMultipleOutgoings.class).get();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();

        await().until(() -> bean.list().size() == 6);
        await().until(() -> inA.list().size() == 6);
        await().until(() -> inB.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
        assertThat(inA.list()).contains("A", "B", "C", "D", "E", "F");
        assertThat(inB.list()).contains("A", "B", "C", "D", "E", "F");
    }

    @Test
    public void testCombiningOutgoingsAndBroadcastWithAProcessor() {
        addBeanClass(MySource.class);
        addBeanClass(IncomingOnA.class);
        addBeanClass(ProcessorUsingMultipleOutgoingsAndBroadcast.class);
        addBeanClass(IncomingOnB.class);

        initialize();
        ProcessorUsingMultipleOutgoingsAndBroadcast bean = container.select(ProcessorUsingMultipleOutgoingsAndBroadcast.class)
                .get();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();

        await().until(() -> bean.list().size() == 6);
        await().until(() -> inA.list().size() == 6);
        await().until(() -> inB.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
        assertThat(inA.list()).contains("A", "B", "C", "D", "E", "F");
        assertThat(inB.list()).contains("A", "B", "C", "D", "E", "F");
    }

    @Test
    public void testCombiningOutgoingsAndBroadcastWithASubscriber() {
        addBeanClass(MySource.class);
        addBeanClass(MySecondSource.class);
        addBeanClass(IncomingOnA.class);
        addBeanClass(MyBeanUsingMultipleIncomingsAndMergeAndMultipleOutgoings.class);
        addBeanClass(IncomingOnB.class);

        initialize();
        MyBeanUsingMultipleIncomingsAndMergeAndMultipleOutgoings bean = container
                .select(MyBeanUsingMultipleIncomingsAndMergeAndMultipleOutgoings.class).get();
        IncomingOnA inA = container.select(IncomingOnA.class).get();
        IncomingOnB inB = container.select(IncomingOnB.class).get();

        await().until(() -> bean.list().size() == 8);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f", "g", "h");
        assertThat(inA.list()).contains("A", "B", "C", "D", "E", "F", "G", "H");
        assertThat(inB.list()).contains("A", "B", "C", "D", "E", "F", "G", "H");
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
    public static class SecondIncomingOnB {

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
    public static class MultipleOutgoingsProducer {

        @Outgoing("a")
        @Outgoing("b")
        @Outgoing("c")
        @Broadcast
        public Publisher<String> produce() {
            return Flowable.just("a", "b", "c", "d", "e", "f");
        }

    }

    @ApplicationScoped
    public static class MyBeanUsingMultipleIncomings {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("a")
        @Incoming("b")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class MyBeanUsingOutgoings {

        @Outgoings({
                @Outgoing("a"),
                @Outgoing("b")
        })
        @Broadcast
        public Publisher<String> produce() {
            return Flowable.just("a", "b", "c", "d", "e", "f");
        }

    }

    @ApplicationScoped
    public static class MyBeanUsingMultipleIncomingsAndMergeAndMultipleOutgoings {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Merge
        @Incoming("in")
        @Incoming("in2")
        @Outgoing("a")
        @Outgoing("b")
        public String consume(String s) {
            list.add(s);
            return s.toUpperCase();
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class ProcessorUsingMultipleOutgoings {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        @Outgoing("a")
        @Outgoing("b")
        public String consume(String s) {
            list.add(s);
            return s.toUpperCase();
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class ProcessorUsingMultipleOutgoingsAndBroadcast {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("in")
        @Outgoing("a")
        @Outgoing("b")
        @Broadcast
        public String consume(String s) {
            list.add(s);
            return s.toUpperCase();
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MySource {

        @Outgoing("in")
        public Multi<String> produce() {
            return Multi.createFrom().items("a", "b", "c", "d", "e", "f");
        }

    }

    @ApplicationScoped
    public static class MySecondSource {

        @Outgoing("in2")
        public Multi<String> produce() {
            return Multi.createFrom().items("g", "h");
        }

    }

    @ApplicationScoped
    public static class InvalidBeanWithOutgoings {

        @Outgoing("a")
        @Outgoing("")
        public Flow.Publisher<String> produce() {
            return Multi.createFrom().items("a", "b", "c");
        }

    }

    @ApplicationScoped
    public static class InvalidOutgoings {

        @Outgoings({
                @Outgoing("a"),
                @Outgoing("")
        })
        public Flow.Publisher<String> produce() {
            return Multi.createFrom().items("a", "b", "c");
        }
    }

    @ApplicationScoped
    public static class EmptyOutgoings {

        @Outgoings({})
        public Flow.Publisher<String> produce() {
            return Multi.createFrom().items("a", "b", "c");
        }
    }

}
