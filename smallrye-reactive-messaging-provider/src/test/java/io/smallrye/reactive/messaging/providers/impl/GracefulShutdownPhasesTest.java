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
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.jboss.logmanager.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.testing.logging.LogCapture;

/**
 * How the shutdown phases behave when things go wrong or run twice: a drain that times out, a connector failing to
 * prepare or close a channel, a nacked in-flight message, and the phases running once.
 */
class GracefulShutdownPhasesTest extends WeldTestBaseWithoutTails {

    @RegisterExtension
    static LogCapture logCapture = LogCapture.with(
            r -> "io.smallrye.reactive.messaging.provider".equals(r.getLoggerName()), Level.WARN);

    @BeforeEach
    void reset() {
        StableConnector.COUNTERS.reset();
        FlakyConnector.COUNTERS.reset();
        FlakyConnector.FAIL_PREPARE = false;
        FlakyConnector.FAIL_CLOSE = false;
        GatedConsumer.reset();
    }

    @AfterEach
    void releaseGate() {
        // Never let a pending message hold up container shutdown.
        GatedConsumer.GATE.complete(null);
        releaseConfig();
    }

    @Test
    void defaultDrainTimeoutAppliesToChannelsWithoutOne() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        props.put("smallrye.messaging.graceful-shutdown.default.drain-timeout", "1500");
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, SimpleConsumer.class);
        initialize();

        assertThat(get(ChannelRegistry.class).getPausable("in").getDrainTimeout()).isEqualTo(Duration.ofMillis(1500));
    }

    @Test
    void channelDrainTimeoutWinsOverTheDefault() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        props.put("mp.messaging.incoming.in.graceful-shutdown.drain-timeout", "700");
        props.put("smallrye.messaging.graceful-shutdown.default.drain-timeout", "1500");
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, SimpleConsumer.class);
        initialize();

        assertThat(get(ChannelRegistry.class).getPausable("in").getDrainTimeout()).isEqualTo(Duration.ofMillis(700));
    }

    @Test
    void drainThatTimesOutDoesNotBlockShutdown() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        props.put("mp.messaging.incoming.in.graceful-shutdown", "true");
        props.put("mp.messaging.incoming.in.graceful-shutdown.drain-timeout", "500");
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, GatedConsumer.class);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !GatedConsumer.RECEIVED.isEmpty());

        // The held message is never acked: the drain gives up after its timeout, and the phase carries on.
        long start = System.nanoTime();
        get(ChannelLifecycleManager.class).prepareCloseIncoming();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
        assertThat(StableConnector.COUNTERS.prepared).containsExactly("in");
        assertThat(StableConnector.COUNTERS.acks).hasValue(0);
        assertThat(logCapture.records())
                .filteredOn(r -> r.getMessage().contains("SRMSG00246"))
                .hasSize(1);
    }

    @Test
    void connectorFailingToPrepareDoesNotStopTheOthers() {
        FlakyConnector.FAIL_PREPARE = true;
        startTwoChannels();

        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        lifecycle.prepareCloseIncoming();
        lifecycle.prepareCloseOutgoing();
        lifecycle.close();

        assertThat(FlakyConnector.COUNTERS.prepared).containsExactly("flaky-in");
        assertThat(StableConnector.COUNTERS.prepared).containsExactly("stable-in");
        // The failure doesn't stop the later phases either.
        assertThat(StableConnector.COUNTERS.closed).containsExactly("stable-in");
        assertThat(FlakyConnector.COUNTERS.closed).containsExactly("flaky-in");
        assertThat(StableConnector.COUNTERS.terminated).hasValue(1);
        assertThat(FlakyConnector.COUNTERS.terminated).hasValue(1);
    }

    @Test
    void connectorFailingToCloseDoesNotStopTheOthers() {
        FlakyConnector.FAIL_CLOSE = true;
        startTwoChannels();

        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        lifecycle.prepareCloseIncoming();
        lifecycle.prepareCloseOutgoing();
        lifecycle.close();

        assertThat(FlakyConnector.COUNTERS.closed).containsExactly("flaky-in");
        assertThat(StableConnector.COUNTERS.closed).containsExactly("stable-in");
        assertThat(StableConnector.COUNTERS.terminated).hasValue(1);
        assertThat(FlakyConnector.COUNTERS.terminated).hasValue(1);
    }

    @Test
    void nackedMessageEndsTheDrain() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        props.put("mp.messaging.incoming.in.graceful-shutdown", "true");
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, NackingConsumer.class);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !GatedConsumer.RECEIVED.isEmpty());
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        CompletableFuture<Void> prepare = CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);

        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());

        // The held message fails once the gate opens: it is nacked, which ends it as an ack would.
        GatedConsumer.GATE.complete(null);
        await().atMost(5, TimeUnit.SECONDS).until(prepare::isDone);
        assertThat(StableConnector.COUNTERS.nacks).hasValue(GatedConsumer.RECEIVED.size());
        assertThat(StableConnector.COUNTERS.acks).hasValue(0);
        assertThat(StableConnector.COUNTERS.prepared).containsExactly("in");
    }

    @Test
    void phasesRunOnceWhenTheContainerIsClosedAfterwards() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, SimpleConsumer.class);
        initialize();

        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        lifecycle.prepareCloseIncoming();
        lifecycle.prepareCloseOutgoing();
        lifecycle.close();

        // The CDI observer runs the phases again on container shutdown: they must be skipped.
        container.close();
        container = null;

        assertThat(StableConnector.COUNTERS.prepared).containsExactly("in");
        assertThat(StableConnector.COUNTERS.closed).containsExactly("in");
        assertThat(StableConnector.COUNTERS.terminated).hasValue(1);
    }

    @Test
    void phasesRunWhenTheConfigIsReleasedBeforeTheContainerIsClosed() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, SimpleConsumer.class);
        initialize();
        await().until(() -> StableConnector.COUNTERS.acks.get() == 3);

        // The configuration is gone when the CDI observer runs the phases: reading the phase timeout must not skip them.
        releaseConfig();
        container.close();
        container = null;

        assertThat(StableConnector.COUNTERS.prepared).containsExactly("in");
        assertThat(StableConnector.COUNTERS.closed).containsExactly("in");
        assertThat(StableConnector.COUNTERS.terminated).hasValue(1);
    }

    @Test
    void asyncPhasesStartWhenSubscribedAndRunOnce() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, SimpleConsumer.class);
        initialize();
        await().until(() -> StableConnector.COUNTERS.acks.get() == 3);
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);

        Uni<Void> prepareIncoming = lifecycle.prepareCloseIncomingAsync();
        // Nothing runs until the Uni is subscribed.
        assertThat(StableConnector.COUNTERS.prepared).isEmpty();
        prepareIncoming.subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5));
        assertThat(StableConnector.COUNTERS.prepared).containsExactly("in");

        // The phase runs once, whichever variant starts it.
        lifecycle.prepareCloseIncoming();
        lifecycle.prepareCloseIncomingAsync().subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5));
        assertThat(StableConnector.COUNTERS.prepared).containsExactly("in");

        lifecycle.prepareCloseOutgoingAsync().subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5));
        lifecycle.closeAsync().subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5));
        lifecycle.close();

        assertThat(StableConnector.COUNTERS.closed).containsExactly("in");
        assertThat(StableConnector.COUNTERS.terminated).hasValue(1);
    }

    @Test
    void asyncPhaseThatTimesOutCompletesTheUni() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", StableConnector.NAME);
        // The phase gives up before the drain (10 s by default).
        props.put("smallrye.messaging.graceful-shutdown.phase-timeout", "500");
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, GatedConsumer.class);
        initialize();
        await().atMost(5, TimeUnit.SECONDS).until(() -> !GatedConsumer.RECEIVED.isEmpty());

        // The held message is never acked: the Uni completes with an item when the phase times out, well before the
        // drain timeout, and the connector is not prepared.
        get(ChannelLifecycleManager.class).prepareCloseIncomingAsync()
                .subscribe().withSubscriber(UniAssertSubscriber.create())
                .awaitItem(Duration.ofSeconds(5));

        assertThat(StableConnector.COUNTERS.prepared).isEmpty();
    }

    private void startTwoChannels() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.stable-in.connector", StableConnector.NAME);
        props.put("mp.messaging.incoming.flaky-in.connector", FlakyConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(StableConnector.class, FlakyConnector.class, TwoChannelsConsumer.class);
        initialize();
    }

    static class Counters {
        final AtomicInteger acks = new AtomicInteger();
        final AtomicInteger nacks = new AtomicInteger();
        final AtomicInteger terminated = new AtomicInteger();
        final List<String> prepared = new CopyOnWriteArrayList<>();
        final List<String> closed = new CopyOnWriteArrayList<>();

        void reset() {
            acks.set(0);
            nacks.set(0);
            terminated.set(0);
            prepared.clear();
            closed.clear();
        }

        Flow.Publisher<? extends Message<?>> messages(Config config) {
            String channel = config.getValue("channel-name", String.class);
            return Multi.createFrom().range(0, 3)
                    .map(i -> Message.of(channel + "-" + i,
                            () -> {
                                acks.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            },
                            failure -> {
                                nacks.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            }));
        }
    }

    @ApplicationScoped
    @Connector(StableConnector.NAME)
    public static class StableConnector implements InboundConnector {

        static final String NAME = "smallrye-stable";
        static final Counters COUNTERS = new Counters();

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return COUNTERS.messages(config);
        }

        @Override
        public CompletionStage<Void> prepareCloseIncoming(String channel) {
            COUNTERS.prepared.add(channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> closeIncoming(String channel) {
            COUNTERS.closed.add(channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> terminate() {
            COUNTERS.terminated.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Fails to prepare (with a failed stage) or to close (by throwing), when asked to.
     */
    @ApplicationScoped
    @Connector(FlakyConnector.NAME)
    public static class FlakyConnector implements InboundConnector {

        static final String NAME = "smallrye-flaky";
        static final Counters COUNTERS = new Counters();
        static volatile boolean FAIL_PREPARE;
        static volatile boolean FAIL_CLOSE;

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            return COUNTERS.messages(config);
        }

        @Override
        public CompletionStage<Void> prepareCloseIncoming(String channel) {
            COUNTERS.prepared.add(channel);
            if (FAIL_PREPARE) {
                return CompletableFuture.failedFuture(new IllegalStateException("prepare failed"));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> closeIncoming(String channel) {
            COUNTERS.closed.add(channel);
            if (FAIL_CLOSE) {
                throw new IllegalStateException("close failed");
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> terminate() {
            COUNTERS.terminated.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Holds each message until the gate opens, then acks it.
     */
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
            return GATE;
        }
    }

    /**
     * Holds each message until the gate opens, then fails it, so it is nacked.
     */
    @ApplicationScoped
    public static class NackingConsumer {

        @Incoming("in")
        public CompletionStage<Void> consume(String payload) {
            GatedConsumer.RECEIVED.add(payload);
            return GatedConsumer.GATE.thenCompose(v -> CompletableFuture.failedFuture(new Exception("boom " + payload)));
        }
    }

    @ApplicationScoped
    public static class SimpleConsumer {

        @Incoming("in")
        public void consume(String payload) {
        }
    }

    @ApplicationScoped
    public static class TwoChannelsConsumer {

        @Incoming("stable-in")
        public void consumeStable(String payload) {
        }

        @Incoming("flaky-in")
        public void consumeFlaky(String payload) {
        }
    }
}
