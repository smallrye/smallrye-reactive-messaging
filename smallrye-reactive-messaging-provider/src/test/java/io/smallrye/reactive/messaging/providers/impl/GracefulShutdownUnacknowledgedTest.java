package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.providers.extension.CompositePausableChannel;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

/**
 * A channel consumed by a method that never acknowledges its messages ({@code @Acknowledgment(NONE)}) does not track
 * them, so its drain does not wait for the drain timeout. Methods acknowledging manually are still waited for.
 */
class GracefulShutdownUnacknowledgedTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void reset() {
        RECEIVED.clear();
    }

    @AfterAll
    static void cleanup() {
        releaseConfig();
    }

    static final List<Message<String>> RECEIVED = new CopyOnWriteArrayList<>();

    @Test
    void channelConsumedWithoutAcknowledgementIsNotTracked() {
        PausableChannel pausable = start(NeverAckingConsumer.class, new Properties());

        assertThat(pausable).isInstanceOf(PauserChannel.class);
        assertThat(((PauserChannel) pausable).isTracking()).isFalse();
        assertThat(((PauserChannel) pausable).getInFlightCount()).isZero();

        // The messages are never acknowledged: the drain must not wait for them.
        CompletableFuture<Void> drain = pausable.pauseAndDrain().subscribeAsCompletionStage();
        await().atMost(2, TimeUnit.SECONDS).until(drain::isDone);
        assertThat(pausable.isPaused()).isTrue();
    }

    @Test
    void channelWithConcurrencyConsumedWithoutAcknowledgementIsNotTracked() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.concurrency", "2");
        PausableChannel pausable = start(NeverAckingConsumer.class, props);

        assertThat(pausable).isInstanceOf(CompositePausableChannel.class);
        for (PausableChannel part : ((CompositePausableChannel) pausable).getParts()) {
            assertThat(((PauserChannel) part).isTracking()).isFalse();
        }

        CompletableFuture<Void> drain = pausable.pauseAndDrain().subscribeAsCompletionStage();
        await().atMost(2, TimeUnit.SECONDS).until(drain::isDone);
    }

    @Test
    void channelConsumedWithManualAcknowledgementIsStillTracked() {
        PausableChannel pausable = start(ManualAckingConsumer.class, new Properties());

        assertThat(((PauserChannel) pausable).isTracking()).isTrue();
        assertThat(((PauserChannel) pausable).getInFlightCount()).isEqualTo(3);

        // The messages will be acknowledged later: the drain waits for them.
        CompletableFuture<Void> drain = pausable.pauseAndDrain().subscribeAsCompletionStage();
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !drain.isDone());

        RECEIVED.forEach(m -> m.ack().toCompletableFuture().join());
        await().atMost(5, TimeUnit.SECONDS).until(drain::isDone);
    }

    private PausableChannel start(Class<?> consumer, Properties props) {
        props.put("mp.messaging.incoming.in.connector", ThreeMessagesConnector.NAME);
        props.put("mp.messaging.incoming.in.graceful-shutdown", "true");
        // Longer than the test timeouts, so a drain waiting for its timeout fails the test.
        props.put("mp.messaging.incoming.in.graceful-shutdown.drain-timeout", "30000");
        installConfigFromProperties(props);
        addBeanClass(ThreeMessagesConnector.class, consumer);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> RECEIVED.size() >= 3);
        PausableChannel pausable = get(ChannelRegistry.class).getPausable("in");
        assertThat(pausable).isNotNull();
        return pausable;
    }

    @ApplicationScoped
    @Connector(ThreeMessagesConnector.NAME)
    public static class ThreeMessagesConnector implements InboundConnector {

        static final String NAME = "smallrye-three-messages";

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("a", "b", "c").map(Message::of);
        }
    }

    @ApplicationScoped
    public static class NeverAckingConsumer {

        @Incoming("in")
        @Acknowledgment(Acknowledgment.Strategy.NONE)
        public CompletionStage<Void> consume(Message<String> message) {
            RECEIVED.add(message);
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class ManualAckingConsumer {

        @Incoming("in")
        @Acknowledgment(Acknowledgment.Strategy.MANUAL)
        public CompletionStage<Void> consume(Message<String> message) {
            RECEIVED.add(message);
            return CompletableFuture.completedFuture(null);
        }
    }
}
