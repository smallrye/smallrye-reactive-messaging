package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.providers.extension.PausableChannelDecorator.PauserChannel;

/**
 * Default of {@code graceful-shutdown} for outgoing connector channels: enabled unless the channel, the connector or
 * the global {@code smallrye.messaging.graceful-shutdown.default.enabled} property disables it. The incoming default a
 * connector declares does not apply to its outgoing channels.
 */
class GracefulShutdownOutgoingDefaultTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void reset() {
        HoldingConnector.RECEIVED.clear();
    }

    @AfterEach
    void ackEverything() {
        // Never let a message in flight hold up the container shutdown.
        HoldingConnector.RECEIVED.forEach(m -> m.ack().toCompletableFuture().join());
    }

    @AfterAll
    static void cleanup() {
        releaseConfig();
    }

    @Test
    void outgoingChannelIsDrainedByDefault() {
        PausableChannel pausable = start(new Properties());

        assertThat(pausable).isInstanceOf(PauserChannel.class);
        assertThat(((PauserChannel) pausable).isTracking()).isTrue();

        // The message reached the connector but is not acknowledged yet: the drain waits for it.
        CompletableFuture<Void> drain = pausable.pauseAndDrain().subscribeAsCompletionStage();
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !drain.isDone());

        HoldingConnector.RECEIVED.forEach(m -> m.ack().toCompletableFuture().join());
        await().atMost(5, TimeUnit.SECONDS).until(drain::isDone);
    }

    @Test
    void channelPropertyDisablesTheDefault() {
        Properties props = new Properties();
        props.put("mp.messaging.outgoing.out." + PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, "false");

        assertThat(start(props)).isNull();
    }

    @Test
    void connectorLevelPropertyDisablesTheDefault() {
        Properties props = new Properties();
        props.put("mp.messaging.connector." + HoldingConnector.NAME + "."
                + PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, "false");

        assertThat(start(props)).isNull();
    }

    @Test
    void globalSwitchDisablesTheDefault() {
        Properties props = new Properties();
        props.put(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY, "false");

        assertThat(start(props)).isNull();
    }

    @Test
    void globalSwitchDoesNotOverrideTheChannelProperty() {
        Properties props = new Properties();
        props.put(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY, "false");
        props.put("mp.messaging.outgoing.out." + PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, "true");

        PausableChannel pausable = start(props);
        assertThat(pausable).isInstanceOf(PauserChannel.class);
        assertThat(((PauserChannel) pausable).isTracking()).isTrue();
    }

    @Test
    void incomingDefaultOfTheConnectorDoesNotApplyToOutgoingChannels() {
        // The connector disables graceful shutdown by default for its incoming channels only.
        PausableChannel pausable = start(new Properties());

        assertThat(new HoldingConnector().isGracefulShutdownEnabled(new BuilderConfig(Map.of()))).isFalse();
        assertThat(pausable).isNotNull();
        assertThat(((PauserChannel) pausable).isTracking()).isTrue();
    }

    private PausableChannel start(Properties props) {
        props.put("mp.messaging.outgoing.out.connector", HoldingConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(HoldingConnector.class, Producer.class);
        initialize();

        get(Producer.class).send("hello");
        await().atMost(5, TimeUnit.SECONDS).until(() -> HoldingConnector.RECEIVED.size() == 1);
        return get(ChannelRegistry.class).getPausable("out");
    }

    @ApplicationScoped
    public static class Producer {

        @Inject
        @Channel("out")
        Emitter<String> emitter;

        public void send(String payload) {
            emitter.send(payload);
        }
    }

    /**
     * Keeps the messages it receives without acknowledging them. Declares {@code graceful-shutdown=false} as the
     * default of its incoming channels.
     */
    @ApplicationScoped
    @Connector(HoldingConnector.NAME)
    public static class HoldingConnector implements InboundConnector, OutboundConnector {

        static final String NAME = "smallrye-holding";
        static final List<Message<?>> RECEIVED = new CopyOnWriteArrayList<>();

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().empty();
        }

        @Override
        public boolean isGracefulShutdownEnabled(Config config) {
            return config.getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, Boolean.class)
                    .orElse(false);
        }

        @Override
        public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
            return new Flow.Subscriber<Message<?>>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Message<?> message) {
                    RECEIVED.add(message);
                }

                @Override
                public void onError(Throwable failure) {
                }

                @Override
                public void onComplete() {
                }
            };
        }
    }
}
