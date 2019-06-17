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

import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.annotations.Stream;

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

    @Test
    public void testWithTwoEmittersConnectedToOneProcessor() {
        TwoEmittersConnectedToProcessor bean = installInitializeAndGet(TwoEmittersConnectedToProcessor.class);
        bean.run();
        assertThat(bean.list()).containsExactly("A", "B", "C");
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloads {
        @Inject
        @Stream("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a").send("b").send("c").complete();
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingMessages {
        @Inject
        @Stream("foo")
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
        @Stream("missing")
        Emitter<Message<String>> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<Message<String>> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingNull {
        @Inject
        @Stream("foo")
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
            emitter.send("a").send("b");
            try {
                emitter.send(null);
            } catch (IllegalArgumentException e) {
                caught = true;
            }
            emitter.send("c").complete();
        }

        @Incoming("foo")
        public void consume(String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingDataAfterTermination {
        @Inject
        @Stream("foo")
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
            emitter.send("a").send("b").complete();
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
        @Stream("foo")
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
            emitter.send("a").send("b").error(new Exception("BOOM"));
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
        @Stream("foo")
        Emitter<String> emitter;
        private List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a").send("b").send("c").complete();
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
        @Stream("foo")
        Emitter<String> emitter1;

        @Inject
        @Stream("foo")
        Emitter<String> emitter2;

        private List<String> list = new CopyOnWriteArrayList<>();

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter1.send("a");
            emitter2.send("b");
            emitter1.send("c").complete();
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
