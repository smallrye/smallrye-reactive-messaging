package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.annotations.Stream;

/**
 * Test checking the legacy Emitter support.
 */
@SuppressWarnings("deprecation")
public class LegacyEmitterInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithPayloads() {
        final MyBeanEmittingPayloads bean = installInitializeAndGet(MyBeanEmittingPayloads.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse(); // Emitter completed
    }

    @Test
    public void testWithPayloadsLegacy() {
        final MyBeanEmittingPayloadsUsingStream bean = installInitializeAndGet(MyBeanEmittingPayloadsUsingStream.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse(); // Emitter completed
    }

    @Test
    public void testWithProcessor() {
        final EmitterConnectedToProcessor bean = installInitializeAndGet(EmitterConnectedToProcessor.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("A", "B", "C");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse(); // Emitter completed
    }

    @Test
    public void testWithMessages() {
        final MyBeanEmittingMessages bean = installInitializeAndGet(MyBeanEmittingMessages.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isFalse();
        assertThat(bean.emitter().isRequested()).isTrue();
    }

    @Test
    public void testWithMessagesLegacy() {
        final MyBeanEmittingMessagesUsingStream bean = installInitializeAndGet(MyBeanEmittingMessagesUsingStream.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isFalse();
        assertThat(bean.emitter().isRequested()).isTrue();
    }

    @Test
    public void testTermination() {
        final MyBeanEmittingDataAfterTermination bean = installInitializeAndGet(
                MyBeanEmittingDataAfterTermination.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse();
        assertThat(bean.isCaught()).isTrue();
    }

    @Test
    public void testTerminationWithError() {
        final MyBeanEmittingDataAfterTerminationWithError bean = installInitializeAndGet(
                MyBeanEmittingDataAfterTerminationWithError.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse();
        assertThat(bean.isCaught()).isTrue();
    }

    @Test
    public void testWithNull() {
        final MyBeanEmittingNull bean = installInitializeAndGet(MyBeanEmittingNull.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.isCaught()).isTrue();
    }

    @Test(expected = IllegalStateException.class)
    public void testWithMissingStream() {
        // The error is only thrown when a message is emitted as the subscription can be delayed.
        installInitializeAndGet(EmitterInjectionTest.BeanWithMissingStream.class).emitter().send(Message.<String>newBuilder().payload("foo").build());
    }

    @Test(expected = IllegalStateException.class)
    public void testWithMissingChannel() {
        // The error is only thrown when a message is emitted as the subscription can be delayed.
        installInitializeAndGet(EmitterInjectionTest.BeanWithMissingChannel.class).emitter().send(Message.<String>newBuilder().payload("foo").build());
    }

    @Test
    public void testWithTwoEmittersConnectedToOneProcessor() {
        final TwoEmittersConnectedToProcessor bean = installInitializeAndGet(TwoEmittersConnectedToProcessor.class);
        bean.run();
        assertThat(bean.list()).containsExactly("A", "B", "C");
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloads {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            emitter.send("c");
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloadsUsingStream {
        @Inject
        @Stream("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            emitter.send("c");
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingMessages {
        @Inject
        @Channel("foo")
        Emitter<Message<String>> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send(Message.<String>newBuilder().payload("a").build());
            emitter.send(Message.<String>newBuilder().payload("b").build());
            emitter.send(Message.<String>newBuilder().payload("c").build());
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingMessagesUsingStream {
        @Inject
        @Channel("foo")
        Emitter<Message<String>> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send(Message.<String>newBuilder().payload("a").build());
            emitter.send(Message.<String>newBuilder().payload("b").build());
            emitter.send(Message.<String>newBuilder().payload("c").build());
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    public static class BeanWithMissingStream {
        @Inject
        @Stream("missing")
        Emitter<Message<String>> emitter;

        public Emitter<Message<String>> emitter() {
            return emitter;
        }
    }

    public static class BeanWithMissingChannel {
        @Inject
        @Channel("missing")
        Emitter<Message<String>> emitter;

        public Emitter<Message<String>> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingNull {
        @Inject
        @Channel("foo")
        @SuppressWarnings("deprecation")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        @SuppressWarnings("deprecation")
        public Emitter<String> emitter() {
            return emitter;
        }

        boolean isCaught() {
            return caught;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            try {
                emitter.send(null);
            } catch (IllegalArgumentException e) {
                caught = true;
            }
            emitter.send("c");
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingDataAfterTermination {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public Emitter<String> emitter() {
            return emitter;
        }

        boolean isCaught() {
            return caught;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            emitter.complete();
            try {
                emitter.send("c");
            } catch (final IllegalStateException e) {
                caught = true;
            }
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingDataAfterTerminationWithError {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public Emitter<String> emitter() {
            return emitter;
        }

        boolean isCaught() {
            return caught;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            emitter.error(new Exception("BOOM"));
            try {
                emitter.send("c");
            } catch (final IllegalStateException e) {
                caught = true;
            }
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class EmitterConnectedToProcessor {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            emitter.send("c");
            emitter.complete();
        }

        @Incoming("foo")
        @Outgoing("bar")
        public String process(final String s) {
            return s.toUpperCase();
        }

        @Incoming("bar")
        public void consume(final String b) {
            list.add(b);
        }
    }

    @ApplicationScoped
    public static class TwoEmittersConnectedToProcessor {
        @Inject
        @Channel("foo")
        Emitter<String> emitter1;

        @Inject
        @Channel("foo")
        Emitter<String> emitter2;

        private final List<String> list = new CopyOnWriteArrayList<>();

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter1.send("a");
            emitter2.send("b");
            emitter1.send("c");
            emitter1.complete();
        }

        @Incoming("foo")
        @Merge
        @Outgoing("bar")
        public String process(final String s) {
            return s.toUpperCase();
        }

        @Incoming("bar")
        public void consume(final String b) {
            list.add(b);
        }
    }

}
