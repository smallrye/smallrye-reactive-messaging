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
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;

/**
 * Verifies that the effective {@code graceful-shutdown} value of an incoming channel is resolved by the connector
 * ({@link InboundConnector#isGracefulShutdownEnabled(Config)}), so a connector-declared default applies when the
 * property is not set.
 */
class GracefulShutdownConnectorDefaultTest extends WeldTestBaseWithoutTails {

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
    void connectorDefaultEnablesDrainWhenPropertyIsNotSet() {
        CompletableFuture<Void> drain = startAndDrain(DrainByDefaultConnector.NAME, new Properties());

        // A message is still in flight, so the drain must wait for it.
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !drain.isDone());

        GatedConsumer.GATE.complete(null);
        await().atMost(5, TimeUnit.SECONDS).until(drain::isDone);
        assertThat(drain).isCompleted();
    }

    @Test
    void channelPropertyOverridesConnectorDefault() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in." + PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, "false");
        // Without graceful shutdown the channel is not pausable by default.
        props.put("mp.messaging.incoming.in." + PausableChannelConfiguration.PAUSABLE_PROPERTY, "true");

        CompletableFuture<Void> drain = startAndDrain(DrainByDefaultConnector.NAME, props);

        // No tracking: the drain returns right after pausing, although a message is still in flight.
        await().atMost(2, TimeUnit.SECONDS).until(drain::isDone);
        assertThat(GatedConsumer.GATE).isNotDone();
    }

    @Test
    void connectorLevelPropertyOverridesConnectorDefault() {
        Properties props = new Properties();
        props.put("mp.messaging.connector." + DrainByDefaultConnector.NAME + "."
                + PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, "false");
        props.put("mp.messaging.incoming.in." + PausableChannelConfiguration.PAUSABLE_PROPERTY, "true");

        CompletableFuture<Void> drain = startAndDrain(DrainByDefaultConnector.NAME, props);

        await().atMost(2, TimeUnit.SECONDS).until(drain::isDone);
        assertThat(GatedConsumer.GATE).isNotDone();
    }

    @Test
    void frameworkDefaultIsDisabledForConnectorsWithoutOverride() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in." + PausableChannelConfiguration.PAUSABLE_PROPERTY, "true");
        CompletableFuture<Void> drain = startAndDrain(PlainConnector.NAME, props);

        await().atMost(2, TimeUnit.SECONDS).until(drain::isDone);
        assertThat(GatedConsumer.GATE).isNotDone();
    }

    @Test
    void channelPropertyEnablesDrainForConnectorsWithoutOverride() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in." + PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, "true");

        CompletableFuture<Void> drain = startAndDrain(PlainConnector.NAME, props);

        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !drain.isDone());

        GatedConsumer.GATE.complete(null);
        await().atMost(5, TimeUnit.SECONDS).until(drain::isDone);
    }

    @Test
    void pausableFollowsTheResolvedGracefulShutdownValue() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", PlainConnector.NAME);
        props.put("mp.messaging.incoming.other.connector", DrainByDefaultConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(DrainByDefaultConnector.class, PlainConnector.class, GatedConsumer.class, OtherConsumer.class);
        initialize();

        ChannelRegistry registry = get(ChannelRegistry.class);
        // Graceful shutdown disabled (framework default): not pausable.
        assertThat(registry.getPausable("in")).isNull();
        // Graceful shutdown enabled by the connector's default: pausable.
        assertThat(registry.getPausable("other")).isNotNull();
    }

    /**
     * Starts the application with the {@code in} channel bound to the given connector, waits until the first message
     * is in flight (blocked by the gate), then starts draining the channel.
     */
    private CompletableFuture<Void> startAndDrain(String connector, Properties props) {
        GatedConsumer.reset();
        props.put("mp.messaging.incoming.in.connector", connector);
        installConfigFromProperties(props);
        addBeanClass(DrainByDefaultConnector.class, PlainConnector.class, GatedConsumer.class);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !GatedConsumer.RECEIVED.isEmpty());

        PausableChannel pausable = get(ChannelRegistry.class).getPausable("in");
        assertThat(pausable).isNotNull();
        return pausable.pauseAndDrain().subscribeAsCompletionStage();
    }

    @ApplicationScoped
    @Connector(DrainByDefaultConnector.NAME)
    public static class DrainByDefaultConnector implements InboundConnector {

        static final String NAME = "smallrye-drain-by-default";

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("a", "b", "c").map(Message::of);
        }

        /**
         * Same shape as a connector declaring {@code @ConnectorAttribute(name = "graceful-shutdown", defaultValue = "true")}.
         */
        @Override
        public boolean isGracefulShutdownEnabled(Config config) {
            return config.getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, Boolean.class)
                    .orElse(true);
        }
    }

    @ApplicationScoped
    @Connector(PlainConnector.NAME)
    public static class PlainConnector implements InboundConnector {

        static final String NAME = "smallrye-plain";

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("a", "b", "c").map(Message::of);
        }
    }

    @ApplicationScoped
    public static class OtherConsumer {

        @Incoming("other")
        public void consume(String payload) {
            // Only consumes the second channel.
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
