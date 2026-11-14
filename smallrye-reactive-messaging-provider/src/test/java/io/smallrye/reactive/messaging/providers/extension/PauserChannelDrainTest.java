package io.smallrye.reactive.messaging.providers.extension;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.providers.impl.BuilderConfig;
import io.smallrye.reactive.messaging.providers.impl.InternalChannelRegistry;

/**
 * Drain semantics of {@link PausableChannelDecorator.PauserChannel} for channels fed by emitters.
 */
class PauserChannelDrainTest {

    private static final BuilderConfig GRACEFUL = new BuilderConfig(Map.of("graceful-shutdown", "true"));

    private InternalChannelRegistry registry;
    private PausableChannelDecorator decorator;

    @BeforeEach
    void setUp() {
        registry = new InternalChannelRegistry();
        decorator = new PausableChannelDecorator();
        decorator.registry = registry;
    }

    @Test
    void emitterDrainWaitsForBufferedMessagesWhenNothingIsInFlight() {
        UnicastProcessor<Message<String>> source = UnicastProcessor.create();
        registry.register("out", StubEmitter.class, new StubEmitter(source::onComplete));

        AssertSubscriber<Message<?>> sink = subscribe(decorator.decorate(Multi.createFrom().publisher(source),
                List.of("out"), GRACEFUL));

        source.onNext(Message.of("a"));
        source.onNext(Message.of("b"));
        source.onNext(Message.of("c"));

        // The sink processes "a" and acks it before asking for more: nothing is in flight, but "b" and "c"
        // are still buffered upstream.
        sink.request(1);
        assertThat(sink.getItems()).hasSize(1);
        sink.getItems().get(0).ack().toCompletableFuture().join();

        CompletableFuture<Void> drain = pauseAndDrain("out");
        assertThat(drain).as("buffered messages must be flushed before the drain completes").isNotDone();

        // The completion is delivered after the buffered messages.
        sink.request(2);
        assertThat(sink.getItems()).hasSize(3);
        sink.assertCompleted();
        assertThat(drain).as("delivered messages are still in flight").isNotDone();

        sink.getItems().get(1).ack().toCompletableFuture().join();
        assertThat(drain).isNotDone();
        sink.getItems().get(2).nack(new Exception("boom")).toCompletableFuture().join();
        assertThat(drain).isCompleted();
    }

    @Test
    void emitterDrainCompletesWhenUpstreamCompletesAfterLastAck() {
        UnicastProcessor<Message<String>> source = UnicastProcessor.create();
        // The emitter completion is not propagated right away: completion arrives after the last ack.
        AtomicBoolean completeRequested = new AtomicBoolean();
        registry.register("out", StubEmitter.class, new StubEmitter(() -> completeRequested.set(true)));

        AssertSubscriber<Message<?>> sink = subscribe(decorator.decorate(Multi.createFrom().publisher(source),
                List.of("out"), GRACEFUL));
        source.onNext(Message.of("a"));
        sink.request(1);

        CompletableFuture<Void> drain = pauseAndDrain("out");
        assertThat(completeRequested).isTrue();

        sink.getItems().get(0).ack().toCompletableFuture().join();
        assertThat(drain).as("upstream has not completed yet").isNotDone();

        source.onComplete();
        assertThat(drain).isCompleted();
    }

    @Test
    void drainCompletesEveryEmitterOfTheChannel() {
        StubEmitter first = new StubEmitter(() -> {
        });
        OtherStubEmitter second = new OtherStubEmitter();
        registry.register("out", StubEmitter.class, first);

        Multi<? extends Message<?>> decorated = decorator.decorate(Multi.createFrom().<Message<String>> empty(),
                List.of("out"), GRACEFUL);
        // Registered after the decoration: emitters are resolved when draining.
        registry.register("out", OtherStubEmitter.class, second);
        subscribe(decorated);

        pauseAndDrain("out").join();

        assertThat(first.completed).isTrue();
        assertThat(second.completed).isTrue();
    }

    @Test
    void emitterDrainDoesNotWaitWhenTheChannelIsNotConsumed() {
        UnicastProcessor<Message<String>> source = UnicastProcessor.create();
        registry.register("out", StubEmitter.class, new StubEmitter(source::onComplete));
        // Decorated but never subscribed: the completion would never be observed.
        decorator.decorate(Multi.createFrom().publisher(source), List.of("out"), GRACEFUL);

        assertThat(pauseAndDrain("out")).isCompleted();
    }

    @Test
    void emitterDrainDoesNotWaitWithoutGracefulShutdown() {
        UnicastProcessor<Message<String>> source = UnicastProcessor.create();
        StubEmitter emitter = new StubEmitter(source::onComplete);
        registry.register("out", StubEmitter.class, emitter);

        AssertSubscriber<Message<?>> sink = subscribe(decorator.decorate(Multi.createFrom().publisher(source),
                List.of("out"), new BuilderConfig(Map.of("pausable", "true"))));
        source.onNext(Message.of("a"));
        sink.request(1);

        assertThat(pauseAndDrain("out")).isCompleted();
        assertThat(emitter.completed).isTrue();
    }

    @Test
    void secondDecorationOfAnEmitterChannelKeepsTheFirstPauseStage() {
        UnicastProcessor<Message<String>> source = UnicastProcessor.create();
        StubEmitter emitter = new StubEmitter(source::onComplete);
        registry.register("out", StubEmitter.class, emitter);

        // Emitter side (publisher decorators), then connector side (subscriber decorators), with the same config.
        Multi<? extends Message<?>> emitterSide = decorator.decorate(Multi.createFrom().publisher(source),
                List.of("out"), GRACEFUL);
        PausableChannel first = registry.getPausable("out");
        Multi<? extends Message<?>> connectorSide = decorator.decorate(emitterSide, List.of("out"), GRACEFUL);

        assertThat(connectorSide).isSameAs(emitterSide);
        assertThat(registry.getPausable("out")).isSameAs(first);

        // The remaining pause stage still completes the emitter and drains.
        AssertSubscriber<Message<?>> sink = subscribe(connectorSide);
        source.onNext(Message.of("a"));
        sink.request(1);
        CompletableFuture<Void> drain = pauseAndDrain("out");
        assertThat(emitter.completed).isTrue();
        sink.getItems().forEach(m -> m.ack().toCompletableFuture().join());
        assertThat(drain).isCompleted();
    }

    @Test
    void channelIsNotPausableByDefaultWithoutGracefulShutdown() {
        Multi<Message<String>> source = Multi.createFrom().empty();

        assertThat(decorator.decorate(source, List.of("plain"), new BuilderConfig(Map.of()))).isSameAs(source);
        assertThat(registry.getPausable("plain")).isNull();

        decorator.decorate(source, List.of("graceful"), GRACEFUL);
        assertThat(registry.getPausable("graceful")).isNotNull();
    }

    private CompletableFuture<Void> pauseAndDrain(String channel) {
        PausableChannel pausable = registry.getPausable(channel);
        assertThat(pausable).isNotNull();
        return pausable.pauseAndDrain().subscribeAsCompletionStage();
    }

    @SuppressWarnings("unchecked")
    private static AssertSubscriber<Message<?>> subscribe(Multi<? extends Message<?>> multi) {
        return ((Multi<Message<?>>) multi).subscribe().withSubscriber(AssertSubscriber.create(0));
    }

    static class StubEmitter implements MessagePublisherProvider<String> {

        private final Runnable onComplete;
        volatile boolean completed;

        StubEmitter(Runnable onComplete) {
            this.onComplete = onComplete;
        }

        @Override
        public Flow.Publisher<Message<? extends String>> getPublisher() {
            return Multi.createFrom().empty();
        }

        @Override
        public void complete() {
            completed = true;
            onComplete.run();
        }
    }

    static class OtherStubEmitter implements MessagePublisherProvider<String> {

        volatile boolean completed;

        @Override
        public Flow.Publisher<Message<? extends String>> getPublisher() {
            return Multi.createFrom().empty();
        }

        @Override
        public void complete() {
            completed = true;
        }
    }
}
