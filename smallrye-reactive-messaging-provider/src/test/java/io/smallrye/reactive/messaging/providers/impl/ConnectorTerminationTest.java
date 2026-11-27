package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutboundConnector;

/**
 * At the end of shutdown, only the connectors used by channels are terminated, each once: connectors without
 * channels may never have been created (their beans are created lazily) and must not be created just to be terminated.
 * A connector whose runtime channels were all closed before shutdown is still terminated.
 */
class ConnectorTerminationTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void reset() {
        UsedConnector.CREATED.set(0);
        UsedConnector.TERMINATED.set(0);
        UnusedConnector.CREATED.set(0);
        UnusedConnector.TERMINATED.set(0);
        BothConnector.CREATED.set(0);
        BothConnector.TERMINATED.set(0);
        RuntimeConnector.CREATED.set(0);
        RuntimeConnector.TERMINATED.set(0);
    }

    @AfterEach
    void cleanup() {
        releaseConfig();
    }

    @Test
    void onlyUsedConnectorsAreTerminatedOnce() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.used-in.connector", UsedConnector.NAME);
        props.put("mp.messaging.incoming.both-in.connector", BothConnector.NAME);
        props.put("mp.messaging.outgoing.both-out.connector", BothConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(UsedConnector.class, UnusedConnector.class, BothConnector.class, Consumers.class, Producer.class);
        initialize();

        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        lifecycle.prepareCloseIncoming();
        lifecycle.prepareCloseOutgoing();
        lifecycle.close();

        assertThat(UsedConnector.TERMINATED).hasValue(1);
        // A connector used in both directions is terminated once.
        assertThat(BothConnector.TERMINATED).hasValue(1);
        // A connector without channels is neither created nor terminated.
        assertThat(UnusedConnector.CREATED).hasValue(0);
        assertThat(UnusedConnector.TERMINATED).hasValue(0);
    }

    @Test
    void connectorOfRuntimeChannelsClosedBeforeShutdownIsTerminated() {
        addBeanClass(RuntimeConnector.class);
        initialize();

        ChannelFactory factory = get(ChannelFactory.class);
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        ChannelRegistry registry = get(ChannelRegistry.class);

        // The only channels using the connector are created at runtime, then closed before shutdown.
        factory.incoming(runtimeIncoming("runtime-in"), String.class).subscribe();
        lifecycle.closeIncoming("runtime-in").await().atMost(Duration.ofSeconds(5));
        assertThat(registry.getConnectorNames()).doesNotContainValue(RuntimeConnector.NAME);
        assertThat(RuntimeConnector.CREATED).hasValue(1);
        assertThat(RuntimeConnector.TERMINATED).hasValue(0);

        lifecycle.prepareCloseIncoming();
        lifecycle.prepareCloseOutgoing();
        lifecycle.close();

        assertThat(RuntimeConnector.TERMINATED).hasValue(1);
    }

    @Test
    void connectorsToTerminateSelectsUsedNamesAndTerminatesSharedBeansOnce() {
        InboundConnector in = new InOnly();
        InboundConnector unusedIn = new InOnly();
        OutboundConnector out = new OutOnly();
        OutboundConnector unusedOut = new OutOnly();
        Both both = new Both();
        InboundConnector splitIn = new InOnly();
        OutboundConnector splitOut = new OutOnly();

        Map<String, InboundConnector> inbound = Map.of("in", in, "both", both, "split", splitIn, "unused-in", unusedIn);
        Map<String, OutboundConnector> outbound = Map.of("out", out, "both", both, "split", splitOut,
                "unused-out", unusedOut);

        assertThat(ChannelLifecycleManagerImpl.connectorsToTerminate(
                List.of("in", "out", "both", "split", "in"), inbound, outbound))
                // "both" is one bean in both maps: once. "split" has two beans: both. "in" listed twice: once.
                .containsExactlyInAnyOrder(in, out, both, splitIn, splitOut);

        assertThat(ChannelLifecycleManagerImpl.connectorsToTerminate(List.of(), inbound, outbound)).isEmpty();
    }

    private static IncomingConnectorConfiguration runtimeIncoming(String channel) {
        var config = new BuilderConfig(Map.of(
                "connector", RuntimeConnector.NAME,
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

    static class InOnly implements InboundConnector {
        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().empty();
        }
    }

    static class OutOnly implements OutboundConnector {
        @Override
        public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
            return AssertSubscriber.create(Long.MAX_VALUE);
        }
    }

    static class Both implements InboundConnector, OutboundConnector {
        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().empty();
        }

        @Override
        public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
            return AssertSubscriber.create(Long.MAX_VALUE);
        }
    }

    @ApplicationScoped
    @Connector(UsedConnector.NAME)
    public static class UsedConnector implements InboundConnector {
        static final String NAME = "smallrye-used";
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger TERMINATED = new AtomicInteger();

        @PostConstruct
        void init() {
            CREATED.incrementAndGet();
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("a").map(Message::of);
        }

        @Override
        public CompletionStage<Void> terminate() {
            TERMINATED.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    @Connector(UnusedConnector.NAME)
    public static class UnusedConnector implements InboundConnector {
        static final String NAME = "smallrye-unused";
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger TERMINATED = new AtomicInteger();

        @PostConstruct
        void init() {
            CREATED.incrementAndGet();
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().empty();
        }

        @Override
        public CompletionStage<Void> terminate() {
            TERMINATED.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    @Connector(BothConnector.NAME)
    public static class BothConnector implements InboundConnector, OutboundConnector {
        static final String NAME = "smallrye-both";
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger TERMINATED = new AtomicInteger();

        @PostConstruct
        void init() {
            CREATED.incrementAndGet();
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("b").map(Message::of);
        }

        @Override
        public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
            return AssertSubscriber.create(Long.MAX_VALUE);
        }

        @Override
        public CompletionStage<Void> terminate() {
            TERMINATED.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    @Connector(RuntimeConnector.NAME)
    public static class RuntimeConnector implements InboundConnector {
        static final String NAME = "smallrye-runtime";
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger TERMINATED = new AtomicInteger();

        @PostConstruct
        void init() {
            CREATED.incrementAndGet();
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return Multi.createFrom().items("r").map(Message::of);
        }

        @Override
        public CompletionStage<Void> terminate() {
            TERMINATED.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    @ApplicationScoped
    public static class Consumers {

        @Incoming("used-in")
        public void consumeUsed(String payload) {
        }

        @Incoming("both-in")
        public void consumeBoth(String payload) {
        }
    }

    @ApplicationScoped
    public static class Producer {

        @Inject
        @Channel("both-out")
        Emitter<String> emitter;
    }
}
