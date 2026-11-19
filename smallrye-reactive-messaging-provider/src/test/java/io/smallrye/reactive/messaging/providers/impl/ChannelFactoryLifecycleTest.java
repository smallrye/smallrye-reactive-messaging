package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.spi.ConnectorLiteral;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutgoingConnectorConfiguration;
import io.smallrye.reactive.messaging.providers.connectors.MyDummyConnector;

/**
 * Channels created at runtime must have unique names, and closing them removes them from the registry, so they can be
 * created again. Also covers the derivation of the shutdown phase timeout, including the waves of channels.
 */
class ChannelFactoryLifecycleTest extends WeldTestBaseWithoutTails {

    @Test
    void creatingAnIncomingChannelTwiceThrows() {
        initialize();
        ChannelFactory factory = get(ChannelFactory.class);

        factory.incoming(incomingConfig("dup-in"), Integer.class);

        assertThatThrownBy(() -> factory.incoming(incomingConfig("dup-in"), Integer.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dup-in");
    }

    @Test
    @SuppressWarnings("unchecked")
    void creatingAnOutgoingChannelTwiceThrows() {
        initialize();
        ChannelFactory factory = get(ChannelFactory.class);

        factory.outgoing(outgoingConfig("dup-out"), Emitter.class);

        assertThatThrownBy(() -> factory.outgoing(outgoingConfig("dup-out")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dup-out");
        assertThatThrownBy(() -> factory.outgoing(outgoingConfig("dup-out"), Emitter.class))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closedIncomingChannelCanBeCreatedAgain() {
        initialize();
        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        factory.incoming(incomingConfig("re-in"), Integer.class);
        assertThat(registry.getPublishers("re-in")).isNotEmpty();

        get(ChannelLifecycleManager.class).closeIncoming("re-in").await().atMost(Duration.ofSeconds(5));

        assertThat(registry.getPublishers("re-in")).isEmpty();
        assertThat(registry.getIncomingConnectorName("re-in")).isNull();
        assertThat(registry.getPausable("re-in")).isNull();

        List<Integer> received = new CopyOnWriteArrayList<>();
        factory.incoming(incomingConfig("re-in"), Integer.class)
                .subscribe(payload -> {
                    received.add(payload);
                });
        await().untilAsserted(() -> assertThat(received).containsExactly(2, 3, 4));
    }

    @Test
    @SuppressWarnings("unchecked")
    void closedOutgoingChannelCanBeCreatedAgain() {
        initialize();
        ChannelFactory factory = get(ChannelFactory.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        factory.outgoing(outgoingConfig("re-out"), Emitter.class);
        assertThat(registry.getSubscribers("re-out")).isNotEmpty();

        get(ChannelLifecycleManager.class).closeOutgoing("re-out").await().atMost(Duration.ofSeconds(5));

        assertThat(registry.getSubscribers("re-out")).isEmpty();
        assertThat(registry.getPublishers("re-out")).isEmpty();
        assertThat(registry.getEmitter("re-out")).isNull();
        assertThat(registry.getOutgoingConnectorName("re-out")).isNull();

        Emitter<String> emitter = factory.outgoing(outgoingConfig("re-out"), Emitter.class);
        emitter.send("again");
        MyDummyConnector connector = container.select(MyDummyConnector.class, ConnectorLiteral.of("smallrye-dummy")).get();
        await().untilAsserted(() -> assertThat(connector.list()).contains("again"));
    }

    @Test
    void phaseTimeoutIsTheLongestDrainTimeoutPlusAMargin() {
        assertThat(ChannelLifecycleManagerImpl.phaseTimeout(Optional.empty(), List.of(), 0))
                .isEqualTo(ChannelLifecycleManagerImpl.DEFAULT_DRAIN_TIMEOUT
                        .plus(ChannelLifecycleManagerImpl.PHASE_TIMEOUT_MARGIN));

        Duration timeout = ChannelLifecycleManagerImpl.phaseTimeout(Optional.empty(),
                List.of(pausable(Duration.ofSeconds(5)), pausable(Duration.ofSeconds(25))), 2);
        assertThat(timeout).isGreaterThan(Duration.ofSeconds(25))
                .isEqualTo(Duration.ofSeconds(25).plus(ChannelLifecycleManagerImpl.PHASE_TIMEOUT_MARGIN));
    }

    @Test
    void phaseTimeoutCoversEveryWaveOfChannels() {
        List<PausableChannel> pausables = List.of(pausable(Duration.ofSeconds(10)));
        Duration margin = ChannelLifecycleManagerImpl.PHASE_TIMEOUT_MARGIN;
        int concurrency = ChannelLifecycleManagerImpl.PHASE_CONCURRENCY;

        // Up to PHASE_CONCURRENCY calls run in a single wave.
        assertThat(ChannelLifecycleManagerImpl.phaseTimeout(Optional.empty(), pausables, concurrency))
                .isEqualTo(Duration.ofSeconds(10).plus(margin));
        // One more call needs a second wave, and so on.
        assertThat(ChannelLifecycleManagerImpl.phaseTimeout(Optional.empty(), pausables, concurrency + 1))
                .isEqualTo(Duration.ofSeconds(20).plus(margin));
        assertThat(ChannelLifecycleManagerImpl.phaseTimeout(Optional.empty(), pausables, 3 * concurrency))
                .isEqualTo(Duration.ofSeconds(30).plus(margin));
    }

    @Test
    void phaseTimeoutPropertyWins() {
        Duration timeout = ChannelLifecycleManagerImpl.phaseTimeout(Optional.of(5000),
                List.of(pausable(Duration.ofSeconds(25))), 100);
        assertThat(timeout).isEqualTo(Duration.ofSeconds(5));
    }

    private static PausableChannel pausable(Duration drainTimeout) {
        return new PausableChannel() {
            @Override
            public boolean isPaused() {
                return false;
            }

            @Override
            public void pause() {
            }

            @Override
            public void resume() {
            }

            @Override
            public Duration getDrainTimeout() {
                return drainTimeout;
            }
        };
    }

    private static IncomingConnectorConfiguration incomingConfig(String channel) {
        var config = new BuilderConfig(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        return new IncomingConnectorConfiguration() {
            @Override
            public Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }

    private static OutgoingConnectorConfiguration outgoingConfig(String channel) {
        var config = new BuilderConfig(Map.of(
                "connector", "dummy",
                "channel-name", channel));
        return new OutgoingConnectorConfiguration() {
            @Override
            public Config config() {
                return config;
            }

            @Override
            public String getChannel() {
                return channel;
            }
        };
    }
}
