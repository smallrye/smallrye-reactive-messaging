package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Merge;

public class MutinyEmitterAndAwaitInjectionTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithPayloads() {
        final MyBeanEmittingPayloads bean = installInitializeAndGet(MyBeanEmittingPayloads.class);
        new Thread(bean::run).start();
        await().until(() -> bean.list().size() == 3);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testWithPayloadsAndAck() {
        final MyBeanEmittingPayloadsWithAck bean = installInitializeAndGet(MyBeanEmittingPayloadsWithAck.class);
        new Thread(bean::run).start();
        await().until(bean::isFinished);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testWithPayloadsAndNack() {
        final MyBeanEmittingPayloadsWithNack bean = installInitializeAndGet(MyBeanEmittingPayloadsWithNack.class);
        new Thread(bean::run).start();
        await().until(() -> bean.list().size() == 4);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.getExceptions()).isNotNull();
        assertThat(bean.getExceptions().size()).isEqualTo(1);
        assertThat(bean.list()).containsExactly("a", "b", "c", "d");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
        assertThat(bean.getExceptions().get(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWithProcessor() {
        final EmitterConnectedToProcessor bean = installInitializeAndGet(EmitterConnectedToProcessor.class);
        new Thread(bean::run).start();
        await().until(() -> bean.list().size() == 3);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("A", "B", "C");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testTermination() {
        final MyBeanEmittingDataAfterTermination bean = installInitializeAndGet(
                MyBeanEmittingDataAfterTermination.class);
        new Thread(bean::run).start();
        await().until(bean::isCaught);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
        assertThat(bean.isCaught()).isTrue();
    }

    @Test
    public void testTerminationWithError() {
        final MyBeanEmittingDataAfterTerminationWithError bean = installInitializeAndGet(
                MyBeanEmittingDataAfterTerminationWithError.class);
        new Thread(bean::run).start();
        await().until(() -> bean.list().size() == 2);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
        assertThat(bean.isCaught()).isTrue();
    }

    @Test
    public void testWithNull() {
        final MyBeanEmittingNull bean = installInitializeAndGet(MyBeanEmittingNull.class);
        new Thread(bean::run).start();
        await().until(() -> bean.list().size() == 3);
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.hasCaughtNullPayload()).isTrue();
    }

    @Test
    public void testWithMissingChannel() {
        assertThatThrownBy(() -> {
            // The error is only thrown when a message is emitted as the subscription can be delayed.
            installInitializeAndGet(BeanWithMissingChannel.class).emitter().sendAndAwait("foo");
        }).isInstanceOf(DefinitionException.class);
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
        new Thread(() -> emitter.sendAndAwait("a")).start();
        new Thread(() -> emitter.sendAndAwait("b")).start();
        new Thread(() -> emitter.sendAndAwait("c")).start();
        await().until(() -> list.size() == 3);
        assertThat(list).containsExactlyInAnyOrder("a", "b", "c");
        new Thread(() -> emitter.sendAndAwait("d")).start();
        await().until(() -> list.size() == 4);
        emitter.complete();
        assertThat(list).containsExactlyInAnyOrder("a", "b", "c", "d");
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
            emitter.sendAndAwait("a");
            emitter.sendAndAwait("b");
            emitter.sendAndAwait("c");
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloadsWithAck {
        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean finished = false;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public boolean isFinished() {
            return finished;
        }

        public void run() {
            emitter.sendAndAwait("a");
            emitter.sendAndAwait("b");
            emitter.sendAndAwait("c");
            emitter.complete();
            finished = true;
        }

        @Incoming("foo")
        @Acknowledgment(Strategy.MANUAL)
        public CompletionStage<Void> consume(final Message<String> s) {
            list.add(s.getPayload());
            return s.ack();
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloadsWithNack {
        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        private final List<Exception> exceptionList = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            try {
                emitter.sendAndAwait("a");
            } catch (Exception ce) {
                exceptionList.add(ce);
            }
            try {
                emitter.sendAndAwait("b");
            } catch (Exception ce) {
                exceptionList.add(ce);
            }
            try {
                emitter.sendAndAwait("c");
            } catch (Exception ce) {
                exceptionList.add(ce);
            }
            try {
                emitter.sendAndAwait("d");
            } catch (Exception ce) {
                exceptionList.add(ce);
            }
            emitter.complete();
        }

        List<Exception> getExceptions() {
            return exceptionList;
        }

        @Incoming("foo")
        @Acknowledgment(Strategy.MANUAL)
        public CompletionStage<Void> consume(final Message<String> s) {
            list.add(s.getPayload());

            if ("c".equals(s.getPayload())) {
                return s.nack(new IllegalArgumentException("c found"));
            } else {
                return s.ack();
            }

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
            emitter.sendAndAwait("a");
            emitter.sendAndAwait("b");
            try {
                emitter.sendAndAwait((String) null);
            } catch (IllegalArgumentException e) {
                caughtNullPayload = true;
            }
            emitter.sendAndAwait("c");
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

        boolean isCaught() {
            return caught;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndAwait("a");
            emitter.sendAndAwait("b");
            emitter.complete();
            try {
                emitter.sendAndAwait("c");
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
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caught;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        boolean isCaught() {
            return caught;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndAwait("a");
            emitter.sendAndAwait("b");
            emitter.error(new Exception("BOOM"));
            try {
                emitter.sendAndAwait("c");
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
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.sendAndAwait("a");
            emitter.sendAndAwait("b");
            emitter.sendAndAwait("c");
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
            emitter1.sendAndAwait("a");
            emitter2.sendAndAwait("b");
            emitter1.sendAndAwait("c");
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
