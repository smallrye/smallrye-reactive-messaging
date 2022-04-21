package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logmanager.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.testing.logging.LogCapture;

@SuppressWarnings("ConstantConditions")
public class MutinyEmitterAndForgetInjectionTest extends WeldTestBaseWithoutTails {

    @RegisterExtension
    static LogCapture logCapture = LogCapture.with(r -> "io.smallrye.reactive.messaging.provider".equals(r.getLoggerName()),
            Level.WARN);

    @BeforeEach
    void clearLogs() {
        logCapture.records().clear();
    }

    @Test
    public void testWithPayloads() {
        final MyBeanEmittingPayloads bean = installInitializeAndGet(MyBeanEmittingPayloads.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testWithProcessor() {
        final EmitterConnectedToProcessor bean = installInitializeAndGet(EmitterConnectedToProcessor.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("A", "B", "C");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testTermination() {
        final MyBeanEmittingDataAfterTermination bean = installInitializeAndGet(
                MyBeanEmittingDataAfterTermination.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testTerminationWithError() {
        final MyBeanEmittingDataAfterTerminationWithError bean = installInitializeAndGet(
                MyBeanEmittingDataAfterTerminationWithError.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testWithNull() {
        final MyBeanEmittingNull bean = installInitializeAndGet(MyBeanEmittingNull.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.hasCaughtNullPayload()).isTrue();
    }

    @Test
    public void testWithMissingChannel() {
        try {
            installInitializeAndGet(BeanWithMissingChannel.class).emitter().sendAndForget("foo");
        } catch (DefinitionException e) {
            assertThat(e).hasMessageContaining("SRMSG00019");
        }
    }

    @Test
    public void testWithTwoEmittersConnectedToOneProcessor() {
        final TwoEmittersConnectedToProcessor bean = installInitializeAndGet(TwoEmittersConnectedToProcessor.class);
        bean.run();
        assertThat(bean.list()).containsExactly("A", "B", "C");
    }

    @SuppressWarnings("ReactiveStreamsSubscriberImplementation")
    @Test
    public void testEmitterAndPublisherInjectedInTheSameClass() {
        EmitterAndPublisher bean = installInitializeAndGet(EmitterAndPublisher.class);
        MutinyEmitter<String> emitter = bean.emitter();
        Publisher<String> publisher = bean.publisher();
        assertThat(emitter).isNotNull();
        assertThat(publisher).isNotNull();
        List<String> list = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();
        publisher.subscribe(new Subscriber<String>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            @Override
            public void onNext(String s) {
                list.add(s);
                subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            @Override
            public void onComplete() {
                completed.set(true);
            }
        });

        assertThat(list).isEmpty();
        assertThat(completed).isFalse();
        emitter.sendAndForget("a");
        emitter.sendAndForget("b");
        emitter.sendAndForget("c");
        assertThat(list).containsExactly("a", "b", "c");
        emitter.sendAndForget("d");
        emitter.complete();
        assertThat(list).containsExactly("a", "b", "c", "d");
        assertThat(completed).isTrue();
    }

    @ApplicationScoped
    public static class EmitterAndPublisher {
        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;

        @Inject
        @Channel("foo")
        Publisher<String> publisher;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public Publisher<String> publisher() {
            return publisher;
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloads {
        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndForget("a");
            emitter.sendAndForget("b");
            emitter.sendAndForget("c");
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    public static class BeanWithMissingChannel {
        @Inject
        @Channel("missing")
        MutinyEmitter<String> emitter;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingNull {
        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caughtNullPayload;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        boolean hasCaughtNullPayload() {
            return caughtNullPayload;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndForget("a");
            emitter.sendAndForget("b");
            try {
                emitter.sendAndForget(null);
            } catch (IllegalArgumentException e) {
                caughtNullPayload = true;
            }
            emitter.sendAndForget("c");
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
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndForget("a");
            emitter.sendAndForget("b");
            emitter.complete();
            emitter.sendAndForget("c");
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
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndForget("a");
            emitter.sendAndForget("b");
            emitter.error(new Exception("BOOM"));
            emitter.sendAndForget("c");
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
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndForget("a");
            emitter.sendAndForget("b");
            emitter.sendAndForget("c");
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
        MutinyEmitter<String> emitter1;

        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter2;

        private final List<String> list = new CopyOnWriteArrayList<>();

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter1.sendAndForget("a");
            emitter2.sendAndForget("b");
            emitter1.sendAndForget("c");
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
