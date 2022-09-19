package io.smallrye.reactive.messaging.inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.providers.helpers.NoStackTraceException;

public class InjectionWithoutSubscriberTest extends WeldTestBaseWithoutTails {

    @Test
    public void testWithoutSubscriber() {
        addBeanClass(AppWithChannel.class);
        AppWithChannel app = installInitializeAndGet(AppWithChannel.class);

        List<String> list = new CopyOnWriteArrayList<>();
        assertThat(app.emitter().hasRequests()).isFalse();
        assertThatThrownBy(() -> app.emitter().send("hello"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no subscriber");

        assertThatThrownBy(() -> app.emitter().send(Message.of("test",
                () -> CompletableFuture.completedFuture(null),
                t -> CompletableFuture.completedFuture(null))))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("no subscriber");

        app.channel().subscribe().with(list::add);

        assertThat(app.emitter().hasRequests()).isTrue();
        app.emitter().send("hello").toCompletableFuture().join();
        app.emitter().send("hello").toCompletableFuture().join();
        app.emitter().send("hello").toCompletableFuture().join();

        assertThat(list).hasSize(3);

    }

    @Test
    public void testWithoutSubscriberWithMutinyEmitter() {
        addBeanClass(MutinyAppWithChannel.class);
        MutinyAppWithChannel app = installInitializeAndGet(MutinyAppWithChannel.class);

        List<String> list = new CopyOnWriteArrayList<>();
        assertThat(app.emitter().hasRequests()).isFalse();
        assertThatThrownBy(() -> app.emitter().send("hello").await().indefinitely())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no subscriber");

        AtomicBoolean acked = new AtomicBoolean();
        AtomicBoolean nacked = new AtomicBoolean();
        app.emitter().send(Message.of("test",
                () -> {
                    acked.set(true);
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    nacked.set(true);
                    return CompletableFuture.completedFuture(null);
                }));
        assertThat(acked).isFalse();
        assertThat(nacked).isTrue();

        app.channel().subscribe().with(list::add);

        assertThat(app.emitter().hasRequests()).isTrue();
        app.emitter().send("hello").await().indefinitely();
        app.emitter().send("hello").await().indefinitely();
        app.emitter().send("hello").await().indefinitely();

        assertThat(list).hasSize(3);

    }

    @Test
    public void testWithoutSubscriberButDropOverflow() {
        addBeanClass(AppWithChannelAndOverflow.class);
        AppWithChannelAndOverflow app = installInitializeAndGet(AppWithChannelAndOverflow.class);

        List<String> list = new CopyOnWriteArrayList<>();
        assertThat(app.emitter().hasRequests()).isFalse();

        app.emitter().send("ok");

        assertThatThrownBy(() -> app.emitter().send("fail because of nack").toCompletableFuture().join())
                .hasCauseInstanceOf(NoStackTraceException.class)
                .hasMessageContaining("Unable to process message");

        AtomicBoolean acked = new AtomicBoolean();
        AtomicBoolean nacked = new AtomicBoolean();
        app.emitter().send(Message.of("test",
                () -> {
                    acked.set(true);
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    nacked.set(true);
                    return CompletableFuture.completedFuture(null);
                }));
        assertThat(acked).isFalse();
        assertThat(nacked).isTrue();

        app.channel().subscribe().with(list::add);

        assertThat(app.emitter().hasRequests()).isTrue();
        app.emitter().send("hello").toCompletableFuture().join();
        app.emitter().send("hello").toCompletableFuture().join();
        app.emitter().send("hello").toCompletableFuture().join();

        assertThat(list).hasSize(3);

    }

    @Test
    public void testWithoutSubscriberButDropOverflowAndUsingMutinyEmitter() {
        addBeanClass(MutinyAppWithChannelAndOverflow.class);
        MutinyAppWithChannelAndOverflow app = installInitializeAndGet(MutinyAppWithChannelAndOverflow.class);

        List<String> list = new CopyOnWriteArrayList<>();
        assertThat(app.emitter().hasRequests()).isFalse();

        app.emitter().send("ok");

        assertThatThrownBy(() -> app.emitter().send("fail because of nack").await().indefinitely())
                .hasCauseInstanceOf(NoStackTraceException.class)
                .hasMessageContaining("Unable to process message");

        AtomicBoolean acked = new AtomicBoolean();
        AtomicBoolean nacked = new AtomicBoolean();
        app.emitter().send(Message.of("test",
                () -> {
                    acked.set(true);
                    return CompletableFuture.completedFuture(null);
                },
                t -> {
                    nacked.set(true);
                    return CompletableFuture.completedFuture(null);
                }));
        assertThat(acked).isFalse();
        assertThat(nacked).isTrue();

        app.channel().subscribe().with(list::add);

        assertThat(app.emitter().hasRequests()).isTrue();
        app.emitter().send("hello").await().indefinitely();
        app.emitter().send("hello").await().indefinitely();
        app.emitter().send("hello").await().indefinitely();

        assertThat(list).hasSize(3);

    }

    @ApplicationScoped
    public static class AppWithChannel {

        @Inject
        @Channel("foo")
        Emitter<String> emitter;

        @Inject
        @Channel("foo")
        Multi<String> channel;

        public Multi<String> channel() {
            return channel;
        }

        public Emitter<String> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MutinyAppWithChannel {

        @Inject
        @Channel("foo")
        MutinyEmitter<String> emitter;

        @Inject
        @Channel("foo")
        Multi<String> channel;

        public Multi<String> channel() {
            return channel;
        }

        public MutinyEmitter<String> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class AppWithChannelAndOverflow {

        @Inject
        @Channel("foo")
        @OnOverflow(OnOverflow.Strategy.DROP)
        Emitter<String> emitter;

        @Inject
        @Channel("foo")
        Multi<String> channel;

        public Multi<String> channel() {
            return channel;
        }

        public Emitter<String> emitter() {
            return emitter;
        }
    }

    @ApplicationScoped
    public static class MutinyAppWithChannelAndOverflow {

        @Inject
        @Channel("foo")
        @OnOverflow(OnOverflow.Strategy.DROP)
        MutinyEmitter<String> emitter;

        @Inject
        @Channel("foo")
        Multi<String> channel;

        public Multi<String> channel() {
            return channel;
        }

        public MutinyEmitter<String> emitter() {
            return emitter;
        }
    }
}
