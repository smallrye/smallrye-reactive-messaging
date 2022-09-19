package io.smallrye.reactive.messaging.incomings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DeploymentException;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Incomings;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.extension.MediatorManager;

public class IncomingsTest extends WeldTestBaseWithoutTails {

    @AfterEach
    public void cleanup() {
        System.clearProperty(MediatorManager.STRICT_MODE_PROPERTY);
    }

    @Test
    public void testInvalidIncomings() {
        addBeanClass(ProducerOnA.class);
        addBeanClass(InvalidBeanWithIncomings.class);

        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);

    }

    @Test
    public void testIncomingsWithTwoSources() {
        addBeanClass(ProducerOnA.class);
        addBeanClass(MyBeanUsingMultipleIncomings.class);
        addBeanClass(ProducerOnB.class);

        initialize();
        MyBeanUsingMultipleIncomings bean = container.select(MyBeanUsingMultipleIncomings.class).get();

        await().until(() -> bean.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testIncomings() {
        addBeanClass(ProducerOnA.class);
        addBeanClass(MyBeanUsingIncomings.class);
        addBeanClass(ProducerOnB.class);

        initialize();
        MyBeanUsingIncomings bean = container.select(MyBeanUsingIncomings.class).get();

        await().until(() -> bean.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testIncomingsWithTwoSourcesReversed() {
        addBeanClass(ProducerOnB.class);
        addBeanClass(MyBeanUsingMultipleIncomings.class);
        addBeanClass(ProducerOnA.class);

        initialize();
        MyBeanUsingMultipleIncomings bean = container.select(MyBeanUsingMultipleIncomings.class).get();

        await().until(() -> bean.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
    }

    @Test
    public void testIncomingsWithMissingSource() {
        addBeanClass(ProducerOnB.class);
        addBeanClass(MyBeanUsingMultipleIncomings.class);
        initialize();
    }

    @Test
    public void testIncomingsWithMissingSourceInStrictMode() {
        System.setProperty(MediatorManager.STRICT_MODE_PROPERTY, "true");
        addBeanClass(ProducerOnB.class);
        addBeanClass(MyBeanUsingMultipleIncomings.class);
        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testProcessorUsingMultipleIncoming() {
        addBeanClass(MySink.class);
        addBeanClass(ProducerOnA.class);
        addBeanClass(ProcessorUsingMultipleIncomings.class);
        addBeanClass(ProducerOnB.class);

        initialize();
        ProcessorUsingMultipleIncomings bean = container.select(ProcessorUsingMultipleIncomings.class).get();
        MySink sink = container.select(MySink.class).get();

        await().until(() -> bean.list().size() == 6);
        await().until(() -> sink.list().size() == 6);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f");
        assertThat(sink.list()).contains("A", "B", "C", "D", "E", "F");
    }

    @Test
    public void testCombiningIncomingsAndMergeWithAProcessor() {
        addBeanClass(MySink.class);
        addBeanClass(ProducerOnA.class);
        addBeanClass(ProcessorUsingMultipleIncomingsAndMerge.class);
        addBeanClass(ProducerOnB.class);
        addBeanClass(SecondProducerForB.class);

        initialize();
        ProcessorUsingMultipleIncomingsAndMerge bean = container.select(ProcessorUsingMultipleIncomingsAndMerge.class).get();
        MySink sink = container.select(MySink.class).get();

        await().until(() -> bean.list().size() == 8);
        await().until(() -> sink.list().size() == 8);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f", "g", "h");
        assertThat(sink.list()).contains("A", "B", "C", "D", "E", "F", "G", "H");
    }

    @Test
    public void testCombiningIncomingsAndMergeWithASubscriber() {
        addBeanClass(MySink.class);
        addBeanClass(ProducerOnA.class);
        addBeanClass(MyBeanUsingMultipleIncomingsAndMerge.class);
        addBeanClass(ProducerOnB.class);
        addBeanClass(SecondProducerForB.class);

        initialize();
        MyBeanUsingMultipleIncomingsAndMerge bean = container.select(MyBeanUsingMultipleIncomingsAndMerge.class).get();

        await().until(() -> bean.list().size() == 8);
        assertThat(bean.list()).contains("a", "b", "c", "d", "e", "f", "g", "h");
    }

    @Test
    public void testEmptyIncomings() {
        addBeanClass(ProducerOnA.class);
        addBeanClass(EmptyIncomings.class);

        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @Test
    public void testIncomingsWithInvalidIncoming() {
        addBeanClass(ProducerOnA.class);
        addBeanClass(InvalidBeanWithIncomings.class);

        assertThatThrownBy(this::initialize).isInstanceOf(DeploymentException.class);
    }

    @ApplicationScoped
    public static class ProducerOnA {

        @Outgoing("a")
        public Publisher<String> produce() {
            return Flowable.just("a", "b", "c");
        }

    }

    @ApplicationScoped
    public static class ProducerOnB {

        @Outgoing("b")
        public Publisher<String> produce() {
            return Flowable.just("d", "e", "f");
        }

    }

    @ApplicationScoped
    public static class SecondProducerForB {

        @Outgoing("b")
        public Publisher<String> produce() {
            return Flowable.just("g", "h");
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
    public static class MyBeanUsingIncomings {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incomings({
                @Incoming("a"),
                @Incoming("b")
        })
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class MyBeanUsingMultipleIncomingsAndMerge {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("a")
        @Incoming("b")
        @Merge
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }

    }

    @ApplicationScoped
    public static class ProcessorUsingMultipleIncomings {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("a")
        @Incoming("b")
        @Outgoing("out")
        public String consume(String s) {
            list.add(s);
            return s.toUpperCase();
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class ProcessorUsingMultipleIncomingsAndMerge {

        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("a")
        @Incoming("b")
        @Outgoing("out")
        @Merge
        public String consume(String s) {
            list.add(s);
            return s.toUpperCase();
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class MySink {
        private final List<String> list = new CopyOnWriteArrayList<>();

        @Incoming("out")
        public void consume(String s) {
            list.add(s);
        }

        public List<String> list() {
            return list;
        }
    }

    @ApplicationScoped
    public static class InvalidBeanWithIncomings {

        @Incoming("a")
        @Incoming("")
        public void consume(String s) {
            // Do nothing
        }

    }

    @ApplicationScoped
    public static class InvalidIncomings {

        @Incomings({
                @Incoming("a"),
                @Incoming("")
        })
        public void consume(String s) {
            // Do nothing
        }
    }

    @ApplicationScoped
    public static class EmptyIncomings {

        @Incomings({})
        public void consume(String s) {
            // Do nothing
        }
    }

}
