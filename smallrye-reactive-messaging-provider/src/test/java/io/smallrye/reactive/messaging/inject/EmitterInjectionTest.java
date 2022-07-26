package io.smallrye.reactive.messaging.inject;

import static io.smallrye.reactive.messaging.annotations.EmitterFactoryFor.Literal.EMITTER;
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
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.*;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.subscribers.TestSubscriber;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Merge;
import io.smallrye.reactive.messaging.providers.DefaultEmitterConfiguration;
import io.smallrye.reactive.messaging.providers.extension.EmitterImpl;
import mutiny.zero.flow.adapters.AdaptersToFlow;

public class EmitterInjectionTest extends WeldTestBaseWithoutTails {

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
        List<CompletionStage<Void>> cs = bean.getCompletionStage();
        assertThat(bean.emitter()).isNotNull();
        assertThat(cs.get(0).toCompletableFuture().isDone()).isTrue();
        assertThat(cs.get(1).toCompletableFuture().isDone()).isTrue();
        assertThat(cs.get(2).toCompletableFuture().isDone()).isFalse();
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
        List<CompletionStage<Void>> cs = bean.getCompletionStage();
        assertThat(bean.emitter()).isNotNull();
        assertThat(cs.get(0).toCompletableFuture().isDone()).isTrue();
        assertThat(cs.get(1).toCompletableFuture().isDone()).isTrue();
        assertThat(cs.get(2).toCompletableFuture().isDone()).isTrue();
        assertThat(cs.get(3).toCompletableFuture().isDone()).isTrue();
        await().until(() -> bean.list().size() == 4);
        assertThat(bean.list()).containsExactly("a", "b", "c", "d");
        assertThat(bean.emitter().isCancelled()).isTrue();
        assertThat(bean.emitter().hasRequests()).isFalse();
        assertThatThrownBy(() -> cs.get(2).toCompletableFuture().join()).isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
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

    @Test
    public void testWithMissingChannel() {
        assertThatThrownBy(() -> {
            // The error is only thrown when a message is emitted as the subscription can be delayed.
            installInitializeAndGet(BeanWithMissingChannel.class).emitter().send(Message.of("foo"));
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
        Emitter<String> emitter = bean.emitter();
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
        emitter.send("a");
        emitter.send("b");
        emitter.send("c");
        assertThat(list).containsExactly("a", "b", "c");
        emitter.send("d");
        emitter.complete();
        assertThat(list).containsExactly("a", "b", "c", "d");
        assertThat(completed).isTrue();
    }

    @ApplicationScoped
    public static class EmitterAndPublisher {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;

        @Inject
        @Channel("foo")
        Publisher<String> publisher;

        public Emitter<String> emitter() {
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
    public static class MyBeanEmittingPayloadsWithAck {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        private final List<CompletionStage<Void>> csList = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            csList.add(emitter.send("a"));
            csList.add(emitter.send("b"));
            csList.add(emitter.send("c"));
            emitter.complete();
        }

        List<CompletionStage<Void>> getCompletionStage() {
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
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        private final List<CompletionStage<Void>> csList = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            csList.add(emitter.send("a"));
            csList.add(emitter.send("b"));
            csList.add(emitter.send("c"));
            csList.add(emitter.send("d"));
            emitter.complete();
        }

        List<CompletionStage<Void>> getCompletionStage() {
            return csList;
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
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
            return emitter;
        }

        public List<String> list() {
            return list;
        }

        public void run() {
            emitter.send(new MyMessageBean<>("a"));
            emitter.send(new MyMessageBean<>("b"));
            emitter.send(new MyMessageBean<>("c"));
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
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();

        public Emitter<String> emitter() {
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
        public void consume(final String s) {
            list.add(s);
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingMessagesUsingStream {
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
            emitter.send(Message.of("a"));
            emitter.send(Message.of("b"));
            emitter.send(Message.of("c"));
        }

        @Incoming("foo")
        public void consume(final String s) {
            list.add(s);
        }
    }

    public static class BeanWithMissingChannel {
        @Inject
        @Channel("missing")
        Emitter<String> emitter;

        public Emitter<String> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MyBeanEmittingNull {
        @Inject
        @Channel("foo")
        Emitter<String> emitter;
        private final List<String> list = new CopyOnWriteArrayList<>();
        private boolean caughtNullPayload;
        private boolean caughtNullMessage;

        public Emitter<String> emitter() {
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
            emitter.send("a");
            emitter.send("b");
            try {
                emitter.send((String) null);
            } catch (IllegalArgumentException e) {
                caughtNullPayload = true;
            }

            try {
                emitter.send((Message<String>) null);
            } catch (IllegalArgumentException e) {
                caughtNullMessage = true;
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

    @Test // Reproduce #511
    public void testWeCanHaveSeveralSubscribers() {
        OnOverflow overflow = new OnOverflow() {
            @Override
            public Class<? extends Annotation> annotationType() {
                return OnOverflow.class;
            }

            @Override
            public Strategy value() {
                return OnOverflow.Strategy.BUFFER;
            }

            @Override
            public long bufferSize() {
                return 128;
            }
        };
        EmitterConfiguration config = new DefaultEmitterConfiguration("my-channel", EMITTER, overflow, null);
        EmitterImpl<String> emitter = new EmitterImpl<>(config, 128);
        Flow.Publisher<Message<? extends String>> publisher = emitter.getPublisher();

        TestSubscriber<Message<? extends String>> sub1 = new TestSubscriber<>();
        publisher.subscribe(AdaptersToFlow.subscriber(sub1));

        TestSubscriber<Message<? extends String>> sub2 = new TestSubscriber<>();
        publisher.subscribe(AdaptersToFlow.subscriber(sub2));

        sub1.assertNoErrors();
        sub2.assertNoErrors();
    }

    @Test
    public void emitterOfMessageInjectionTest() {
        addBeanClass(EmitterOfMessageComponent.class);
        initialize();
        assertThatThrownBy(() -> {
            EmitterOfMessageComponent component = get(EmitterOfMessageComponent.class);
            component.emit("foo");
        })
                .isInstanceOf(DefinitionException.class)
                .hasMessageContaining("Emitter<T>");
    }

    @Test
    public void injectionOfRawEmitterTest() {
        addBeanClass(EmitterRawComponent.class);
        assertThatThrownBy(() -> {
            initialize();
            EmitterRawComponent component = get(EmitterRawComponent.class);
            component.emit("foo");
        })
                .isInstanceOf(DefinitionException.class)
                .hasMessageContaining("emitted type");
    }

    @Test
    public void injectionOfRawMessageEmitterTest() {
        addBeanClass(EmitterOfRawMessageComponent.class);
        initialize();
        assertThatThrownBy(() -> {
            EmitterOfRawMessageComponent component = get(EmitterOfRawMessageComponent.class);
            component.emit("foo");
        })
                .isInstanceOf(DefinitionException.class)
                .hasMessageContaining("Emitter<T>");
    }

    @ApplicationScoped
    public static class EmitterOfMessageComponent {
        @Inject
        @Channel("foo")
        Emitter<Message<String>> emitter;

        public void emit(String s) {
            emitter.send(Message.of(s));
        }

        @SuppressWarnings("unused")
        @Incoming("foo")
        public void consume(String s) {
            // Do nothing - just here to close the graph
        }
    }

    @SuppressWarnings("rawtypes")
    @ApplicationScoped
    public static class EmitterRawComponent {
        @Inject
        @Channel("foo")
        Emitter emitter;

        @SuppressWarnings("unchecked")
        public void emit(String s) {
            emitter.send(Message.of(s));
        }

        @SuppressWarnings("unused")
        @Incoming("foo")
        public void consume(String s) {
            // Do nothing - just here to close the graph
        }
    }

    @SuppressWarnings("rawtypes")
    @ApplicationScoped
    public static class EmitterOfRawMessageComponent {
        @Inject
        @Channel("foo")
        Emitter<Message> emitter;

        public void emit(String s) {
            emitter.send(Message.of(s));
        }

        @SuppressWarnings("unused")
        @Incoming("foo")
        public void consume(String s) {
            // Do nothing - just here to close the graph
        }
    }

}
