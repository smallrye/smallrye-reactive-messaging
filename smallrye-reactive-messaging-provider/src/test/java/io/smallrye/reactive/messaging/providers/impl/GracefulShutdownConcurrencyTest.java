package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.providers.extension.CompositePausableChannel;

/**
 * An incoming channel configured with {@code concurrency} is consumed through indexed channels ({@code in$1}, ...),
 * which the connector knows by their indexed names. Graceful shutdown must drain the channel as a whole and call the
 * connector's per-channel hooks for every indexed channel.
 */
class GracefulShutdownConcurrencyTest extends WeldTestBaseWithoutTails {

    private static final Set<String> INDEXED = Set.of("in$1", "in$2", "in$3");

    @BeforeEach
    void setup() {
        GatedConsumer.reset();
        RecordingConnector.reset();

        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", RecordingConnector.NAME);
        props.put("mp.messaging.incoming.in.concurrency", "3");
        props.put("mp.messaging.incoming.in.graceful-shutdown", "true");
        installConfigFromProperties(props);
        addBeanClass(RecordingConnector.class, GatedConsumer.class);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !GatedConsumer.RECEIVED.isEmpty());
    }

    @AfterEach
    void releaseGate() {
        // Never let a pending message hold up container shutdown.
        GatedConsumer.GATE.complete(null);
    }

    @AfterAll
    static void cleanup() {
        releaseConfig();
    }

    @Test
    void pausableUnderBaseNameControlsEveryIndexedChannel() {
        ChannelRegistry registry = get(ChannelRegistry.class);
        PausableChannel pausable = registry.getPausable("in");

        assertThat(pausable).isInstanceOf(CompositePausableChannel.class);
        assertThat(((CompositePausableChannel) pausable).getParts()).hasSize(3);

        pausable.pause();
        for (String indexed : INDEXED) {
            assertThat(registry.getPausable(indexed).isPaused()).as(indexed).isTrue();
        }
        assertThat(pausable.isPaused()).isTrue();

        pausable.resume();
        for (String indexed : INDEXED) {
            assertThat(registry.getPausable(indexed).isPaused()).as(indexed).isFalse();
        }
    }

    @Test
    void shutdownDrainsTheChannelThenPreparesAndClosesEveryIndexedChannel() {
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);

        // prepareCloseIncoming() blocks until the channel is drained, so run it on another thread.
        CompletableFuture<Void> prepare = CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);

        // A message is in flight on one of the indexed channels: the drain waits, and no channel is prepared yet.
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());
        assertThat(RecordingConnector.PREPARED).isEmpty();

        GatedConsumer.GATE.complete(null);
        await().atMost(10, TimeUnit.SECONDS).until(prepare::isDone);
        assertThat(RecordingConnector.PREPARED).containsExactlyInAnyOrderElementsOf(INDEXED);

        lifecycle.close();
        assertThat(RecordingConnector.CLOSED).containsExactlyInAnyOrderElementsOf(INDEXED);
    }

    @ApplicationScoped
    @Connector(RecordingConnector.NAME)
    public static class RecordingConnector implements InboundConnector {

        static final String NAME = "smallrye-recording";
        static final List<String> PREPARED = new CopyOnWriteArrayList<>();
        static final List<String> CLOSED = new CopyOnWriteArrayList<>();

        static void reset() {
            PREPARED.clear();
            CLOSED.clear();
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("a", "b", "c").map(Message::of);
        }

        @Override
        public CompletionStage<Void> prepareCloseIncoming(String channel) {
            PREPARED.add(channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> closeIncoming(String channel) {
            CLOSED.add(channel);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class GatedConsumer {

        static volatile CompletableFuture<Void> GATE = new CompletableFuture<>();
        static final List<String> RECEIVED = new CopyOnWriteArrayList<>();

        static void reset() {
            GATE = new CompletableFuture<>();
            RECEIVED.clear();
        }

        @Incoming("in")
        public CompletionStage<Void> consume(String payload) {
            RECEIVED.add(payload);
            // The message is acked (and leaves the in-flight count) only once the gate opens.
            return GATE;
        }
    }
}
