package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.DeploymentException;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;

import io.smallrye.reactive.messaging.Emitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.Merge;

public class EmitterInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithPayloads() {
        MyBeanEmittingPayloads bean = installInitializeAndGet(MyBeanEmittingPayloads.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse(); // Emitter completed
    }

    @Test
    public void testWithPayloadsLegacy() {
        MyBeanEmittingPayloadsUsingStream bean = installInitializeAndGet(MyBeanEmittingPayloadsUsingStream.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse(); // Emitter completed
    }

    @Test
    public void testWithProcessor() {
        EmitterConnectedToProcessor bean = installInitializeAndGet(EmitterConnectedToProcessor.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("A", "B", "C");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse(); // Emitter completed
    }

    @Test
    public void testWithMessages() {
        MyBeanEmittingMessages bean = installInitializeAndGet(MyBeanEmittingMessages.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isFalse();
        assertThat(bean.emitter().isRequested()).isTrue();
    }

    @Test
    public void testWithMessagesLegacy() {
        MyBeanEmittingMessagesUsingStream bean = installInitializeAndGet(MyBeanEmittingMessagesUsingStream.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isFalse();
        assertThat(bean.emitter().isRequested()).isTrue();
    }

    @Test
    public void testTermination() {
        MyBeanEmittingDataAfterTermination bean = installInitializeAndGet(MyBeanEmittingDataAfterTermination.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().isRequested()).isFalse();
        assertThat(bean.isCaught()).isTrue();
    }

    @Test
    public void testTerminationWithError() {
        MyBeanEmittingDataAfterTerminationWithError bean = installInitializeAndGet(
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
        MyBeanEmittingNull bean = installInitializeAndGet(MyBeanEmittingNull.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.isCaught()).isTrue();
    }

    @Test(expected = DeploymentException.class)
    public void testWithMissingStream() {
        installInitializeAndGet(BeanWithMissingStream.class);
    }

    @Test(expected = DeploymentException.class)
    public void testWithMissingChannel() {
        installInitializeAndGet(BeanWithMissingChannel.class);
    }

    @Test
    public void testWithTwoEmittersConnectedToOneProcessor() {
        TwoEmittersConnectedToProcessor bean = installInitializeAndGet(TwoEmittersConnectedToProcessor.class);
        bean.run();
        assertThat(bean.list()).containsExactly("A", "B", "C");
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloads {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

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
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloadsUsingStream {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

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
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingMessages {
        @Inject
        @Channel("foo")
        Emitter<Message<String>> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send(Message.of("a"));
            emitter.send(Message.of("b"));
            emitter.send(Message.of("c"));
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingMessagesUsingStream {
        @Inject
        @Channel("foo")
        Emitter<Message<String>> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send(Message.of("a"));
            emitter.send(Message.of("b"));
            emitter.send(Message.of("c"));
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    public static class BeanWithMissingStream {
        @Inject
        @Channel("missing")
        Emitter<Message<String>> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }
    }

    public static class BeanWithMissingChannel {
        @Inject
        @Channel("missing")
        Emitter<Message<String>> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingNull {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public Emitter<String> emitter() {
            return emitter;
        }

        public boolean isCaught() {
            return true;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a");
            emitter.send("b");
            /*
             * try {
             * emitter.send(null);
             * } catch (IllegalArgumentException e) {
             * caught = true;
             * }
             */
            emitter.send("c");
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingDataAfterTermination {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public Emitter<String> emitter() {
            return emitter;
        }

        public boolean isCaught() {
            return true;
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
            } catch (IllegalStateException e) {
                caught = true;
            }
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingDataAfterTerminationWithError {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public Emitter<String> emitter() {
            return emitter;
        }

        public boolean isCaught() {
            return true;
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
            } catch (IllegalStateException e) {
                caught = true;
            }
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class EmitterConnectedToProcessor {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

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
        public String process(String s) {
            return s.toUpperCase();
        }

        @Incoming("bar")
        public void consume(String b) {
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

        private List<String> list = new CopyOnWriteArrayList<>();

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
        public String process(String s) {
            return s.toUpperCase();
        }

        @Incoming("bar")
        public void consume(String b) {
            list.add(b);
        }
    }

}
