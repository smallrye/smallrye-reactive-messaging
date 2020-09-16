package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.subscribers.TestSubscriber;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.extension.EmitterConfiguration;
import io.smallrye.reactive.messaging.extension.MutinyEmitterImpl;

public class MutinyEmitterInjectionTest extends WeldTestBaseWithoutTails {

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
    public void testWithPayloadsAndAck() {
        final MyBeanEmittingPayloadsWithAck bean = installInitializeAndGet(MyBeanEmittingPayloadsWithAck.class);
        bean.run();
        List<Uni<Void>> unis = bean.getUnis();
        assertThat(bean.emitter()).isNotNull();
        assertThat(unis.get(0).subscribeAsCompletionStage().isDone()).isTrue();
        assertThat(unis.get(1).subscribeAsCompletionStage().isDone()).isTrue();
        assertThat(unis.get(2).subscribeAsCompletionStage().isDone()).isFalse();
        bean.emitter().complete();

        await().until(() -> bean.list().size() == 3);
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testMyMessageBeanWithPayloadsAndAck() {
        final MyMessageBeanEmittingPayloadsWithAck bean = installInitializeAndGet(
                MyMessageBeanEmittingPayloadsWithAck.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
    }

    @Test
    public void testWithPayloadsAndNack() {
        final MyBeanEmittingPayloadsWithNack bean = installInitializeAndGet(MyBeanEmittingPayloadsWithNack.class);
        bean.run();
        List<Uni<Void>> unis = bean.getUnis();
        assertThat(bean.emitter()).isNotNull();
        assertThat(unis.get(0).subscribeAsCompletionStage().isDone()).isTrue();
        assertThat(unis.get(1).subscribeAsCompletionStage().isDone()).isTrue();
        assertThat(unis.get(2).subscribeAsCompletionStage().isDone()).isTrue();
        assertThat(unis.get(3).subscribeAsCompletionStage().isDone()).isTrue();
        bean.emitter().complete();
        await().until(() -> bean.list().size() == 4);
        assertThat(bean.list()).containsExactly("a", "b", "c", "d");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
        assertThatThrownBy(() -> unis.get(2).subscribeAsCompletionStage().join()).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
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
    public void testWithMessages() {
        final MyBeanEmittingMessages bean = installInitializeAndGet(MyBeanEmittingMessages.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isFalse();
        assertThat(bean.emitter().hasRequests()).isTrue();
    }

    @Test
    public void testWithMessagesLegacy() {
        final MyBeanEmittingMessagesUsingStream bean = installInitializeAndGet(MyBeanEmittingMessagesUsingStream.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.emitter().isCancelled()).isFalse();
        assertThat(bean.emitter().hasRequests()).isTrue();
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
        assertThat(bean.emitter().hasRequests()).isFalse();
        assertThat(bean.isCaught()).isTrue();
    }

    @Test
    public void testWithNull() {
        final MyBeanEmittingNull bean = installInitializeAndGet(MyBeanEmittingNull.class);
        bean.run();
        assertThat(bean.emitter()).isNotNull();
        assertThat(bean.list()).containsExactly("a", "b", "c");
        assertThat(bean.hasCaughtNullPayload()).isTrue();
        assertThat(bean.hasCaughtNullMessage()).isTrue();
    }

    @Test(expected = IllegalStateException.class)
    public void testWithMissingChannel() {
        // The error is only thrown when a message is emitted as the subscription can be delayed.
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        installInitializeAndGet(BeanWithMissingChannel.class).emitter().send(Message.of("foo")).subscribe().with(x -> {
        }, exception::set);
        await().until(() -> exception.get() != null);
        if (exception.get() instanceof IllegalStateException) {
            throw (IllegalStateException) exception.get();
        }
    }

    @Test
    public void testWithTwoEmittersConnectedToOneProcessor() {
        final TwoEmittersConnectedToProcessor bean = installInitializeAndGet(TwoEmittersConnectedToProcessor.class);
        bean.run();
        assertThat(bean.list()).containsExactly("A", "B", "C");
    }

    @Test
    public void testEmitterAndPublisherInjectedInTheSameClass() {
        EmitterAndPublisher bean = installInitializeAndGet(EmitterAndPublisher.class);
        MutinyEmitter<String> emitter = bean.emitter();
        Publisher<String> publisher = bean.publisher();
        assertThat(emitter).isNotNull();
        assertThat(publisher).isNotNull();
        List<String> list = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean();
        //noinspection SubscriberImplementation
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
        emitter.send("a").subscribe().with(x -> {
        });
        emitter.send("b").subscribe().with(x -> {
        });
        emitter.send("c").subscribe().with(x -> {
        });
        assertThat(list).containsExactly("a", "b", "c");
        emitter.send("d").subscribe().with(x -> {
        });
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
            emitter.send("a").subscribe().with(x -> {
            });
            emitter.send("b").subscribe().with(x -> {
            });
            emitter.send("c").subscribe().with(x -> {
            });
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

        private final List<Uni<Void>> csList = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            csList.add(emitter.send("a"));
            csList.add(emitter.send("b"));
            csList.add(emitter.send("c"));
        }

        List<Uni<Void>> getUnis() {
            return csList;
        }

        @Incoming("foo")
        @Acknowledgment(Strategy.MANUAL)
        public CompletionStage<Void> consume(final Message<String> s) {
            list.add(s.getPayload());

            if (!"c".equals(s.getPayload())) {
                return s.ack();
            } else {
                return new CompletableFuture<>();
            }
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingPayloadsWithNack {
        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        private final List<Uni<Void>> unis = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            unis.add(emitter.send("a"));
            unis.add(emitter.send("b"));
            unis.add(emitter.send("c"));
            unis.add(emitter.send("d"));
        }

        List<Uni<Void>> getUnis() {
            return unis;
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

    @ApplicationScoped
    public static class MyMessageBeanEmittingPayloadsWithAck {
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
            emitter.send(new MyMessageBean<>("a")).subscribe().with(x -> {
            });
            emitter.send(new MyMessageBean<>("b")).subscribe().with(x -> {
            });
            emitter.send(new MyMessageBean<>("c")).subscribe().with(x -> {
            });
            emitter.complete();
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    public static class MyMessageBean<T> implements Message<T> {

        private final T payload;

        MyMessageBean(T payload) {
            this.payload = payload;
        }

        @Override
        public T getPayload() {
            return payload;
        }

    }

    @ApplicationScoped
    public static class MyBeanEmittingMessages {
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
            emitter.send(Message.of("a")).subscribe().with(x -> {
            });
            emitter.send(Message.of("b")).subscribe().with(x -> {
            });
            emitter.send(Message.of("c")).subscribe().with(x -> {
            });

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
        MutinyEmitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send(Message.of("a")).subscribe().with(x -> {
            });
            emitter.send(Message.of("b")).subscribe().with(x -> {
            });
            emitter.send(Message.of("c")).subscribe().with(x -> {
            });
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    public static class BeanWithMissingChannel {
        @Inject
        @Channel("missing")
        MutinyEmitter<Message<String>> emitter;

        public MutinyEmitter<Message<String>> emitter() {
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
        private boolean caughtNullMessage;

        public MutinyEmitter<String> emitter() {
            return emitter;
        }

        boolean hasCaughtNullPayload() {
            return caughtNullPayload;
        }

        boolean hasCaughtNullMessage() {
            return caughtNullMessage;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send("a").subscribe().with(x -> {
            });
            emitter.send("b").subscribe().with(x -> {
            });
            try {
                emitter.send((String) null).subscribe().with(x -> {
                });
            } catch (IllegalArgumentException e) {
                caughtNullPayload = true;
            }

            try {
                emitter.send((Message<String>) null).subscribe().with(x -> {
                });
            } catch (IllegalArgumentException e) {
                caughtNullMessage = true;
            }
            emitter.send("c").subscribe().with(x -> {
            });
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
            emitter.send("a").subscribe().with(x -> {
            });
            emitter.send("b").subscribe().with(x -> {
            });
            emitter.complete();
            emitter.send("c").subscribe().with(x -> {
            }, e -> caught = true);
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
            emitter.send("a").subscribe().with(x -> {
            });
            emitter.send("b").subscribe().with(x -> {
            });
            emitter.error(new Exception("BOOM"));
            emitter.send("c").subscribe().with(x -> {
            }, e -> caught = true);
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
            emitter.send("a").subscribe().with(x -> {
            });
            emitter.send("b").subscribe().with(x -> {
            });
            emitter.send("c").subscribe().with(x -> {
            });
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
            emitter1.send("a").subscribe().with(x -> {
            });
            emitter2.send("b").subscribe().with(x -> {
            });
            emitter1.send("c").subscribe().with(x -> {
            });
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

    @Test // Reproduce #511
    public void testWeCanHaveSeveralSubscribers() {
        OnOverflow overflow = new OnOverflow() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return OnOverflow.class;
            }

            @Override
            public Strategy value() {
                return Strategy.BUFFER;
            }

            @Override
            public long bufferSize() {
                return 128;
            }
        };
        EmitterConfiguration config = new EmitterConfiguration("my-channel", true, overflow, null);
        MutinyEmitterImpl<String> emitter = new MutinyEmitterImpl<>(config, 128);
        Publisher<Message<? extends String>> publisher = emitter.getPublisher();

        TestSubscriber<Message<? extends String>> sub1 = new TestSubscriber<>();
        publisher.subscribe(sub1);

        TestSubscriber<Message<? extends String>> sub2 = new TestSubscriber<>();
        publisher.subscribe(sub2);

        sub1.assertNoErrors();
        sub2.assertNoErrors();
    }

}
