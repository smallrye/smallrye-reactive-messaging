package io.smallrye.reactive.messaging.providers.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ChannelFactory;
import io.smallrye.reactive.messaging.ChannelLifecycleManager;
import io.smallrye.reactive.messaging.WeldTestBaseWithoutTails;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.IncomingConnectorConfiguration;
import io.smallrye.reactive.messaging.connector.OutboundConnector;

/**
 * End-to-end graceful shutdown scenarios: a processing pipeline, several messages in flight at once, a channel
 * created at runtime, and more channels than a phase drains at once.
 */
class GracefulShutdownPipelineTest extends WeldTestBaseWithoutTails {

    @BeforeEach
    void reset() {
        PipelineConnector.reset();
        Gate.reset();
    }

    @AfterEach
    void releaseGates() {
        // Never let a pending message hold up container shutdown.
        Gate.releaseAll();
        releaseConfig();
    }

    @Test
    void pipelineIsDrainedWhileTheOutgoingChannelStillDelivers() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", PipelineConnector.NAME);
        props.put("mp.messaging.incoming.in.graceful-shutdown", "true");
        props.put("mp.messaging.outgoing.out.connector", PipelineConnector.NAME);
        installConfigFromProperties(props);
        addBeanClass(PipelineConnector.class, GatedProcessor.class);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !Gate.RECEIVED.isEmpty());
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);

        // prepareCloseIncoming() blocks until the incoming channel is drained, so run it on another thread.
        CompletableFuture<Void> prepare = CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);

        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());
        assertThat(PipelineConnector.PREPARED_IN).isEmpty();
        assertThat(PipelineConnector.SINK).isEmpty();
        assertThat(PipelineConnector.PREPARED_OUT).isEmpty();
        assertThat(PipelineConnector.CLOSED_OUT).isEmpty();

        Gate.GATE.complete(null);
        await().atMost(10, TimeUnit.SECONDS).until(prepare::isDone);

        // While the incoming channel was drained, the outgoing channel kept delivering: every message that entered
        // the processor reached the sink, and was acked through it. (The processor handles one message at a time,
        // so messages not yet delivered when the channel was paused stay paused and are not acked.)
        List<String> expected = Gate.RECEIVED.stream().map(String::toUpperCase).toList();
        assertThat(expected).isNotEmpty();
        assertThat(PipelineConnector.SINK).containsExactlyElementsOf(expected);
        assertThat(PipelineConnector.ACKS).hasValue(expected.size());
        assertThat(PipelineConnector.PREPARED_IN).containsExactly("in");
        assertThat(PipelineConnector.PREPARED_OUT).isEmpty();

        lifecycle.prepareCloseOutgoing();
        assertThat(PipelineConnector.PREPARED_OUT).containsExactly("out");

        lifecycle.close();
        assertThat(PipelineConnector.CLOSED_IN).containsExactly("in");
        assertThat(PipelineConnector.CLOSED_OUT).containsExactly("out");
    }

    @Test
    void drainWaitsForEveryMessageInFlight() {
        Properties props = new Properties();
        props.put("mp.messaging.incoming.in.connector", PipelineConnector.NAME);
        props.put("mp.messaging.incoming.in.graceful-shutdown", "true");
        installConfigFromProperties(props);
        addBeanClass(PipelineConnector.class, UnorderedBlockingConsumer.class);
        initialize();

        // The three messages are processed at the same time.
        await().atMost(5, TimeUnit.SECONDS).until(() -> Gate.RECEIVED.size() == 3);
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        CompletableFuture<Void> prepare = CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);

        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());

        // Two of three done: still waiting for the last one.
        Gate.release("in-0");
        Gate.release("in-1");
        await().until(() -> PipelineConnector.ACKS.get() == 2);
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());
        assertThat(PipelineConnector.PREPARED_IN).isEmpty();

        Gate.release("in-2");
        await().atMost(10, TimeUnit.SECONDS).until(prepare::isDone);
        assertThat(PipelineConnector.ACKS).hasValue(3);
        assertThat(PipelineConnector.PREPARED_IN).containsExactly("in");
    }

    @Test
    void channelCreatedAtRuntimeIsDrainedOnShutdown() {
        addBeanClass(PipelineConnector.class);
        initialize();

        get(ChannelFactory.class).incoming(incomingConfig("runtime-in"), String.class)
                .subscribeAsync(payload -> {
                    Gate.RECEIVED.add(payload);
                    return Uni.createFrom().completionStage(Gate.GATE);
                });
        await().atMost(5, TimeUnit.SECONDS).until(() -> !Gate.RECEIVED.isEmpty());

        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        CompletableFuture<Void> prepare = CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);

        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());
        assertThat(PipelineConnector.PREPARED_IN).isEmpty();

        Gate.GATE.complete(null);
        await().atMost(10, TimeUnit.SECONDS).until(prepare::isDone);
        // Every message delivered to the binding was acked before the channel was prepared.
        assertThat(PipelineConnector.ACKS).hasValue(Gate.RECEIVED.size());
        assertThat(PipelineConnector.PREPARED_IN).containsExactly("runtime-in");
    }

    @Test
    void moreChannelsThanDrainedAtOnceAreAllDrainedAndPrepared() {
        // A phase drains at most 10 channels at a time: use 12.
        List<String> channels = IntStream.range(0, 12).mapToObj(i -> "in-" + i).toList();
        Properties props = new Properties();
        for (String channel : channels) {
            props.put("mp.messaging.incoming." + channel + ".connector", PipelineConnector.NAME);
            props.put("mp.messaging.incoming." + channel + ".graceful-shutdown", "true");
            props.put("mp.messaging.incoming." + channel + ".count", "1");
        }
        installConfigFromProperties(props);
        addBeanClass(PipelineConnector.class, TwelveChannelsConsumer.class);
        initialize();

        await().atMost(5, TimeUnit.SECONDS).until(() -> !Gate.RECEIVED.isEmpty());
        ChannelLifecycleManager lifecycle = get(ChannelLifecycleManager.class);
        CompletableFuture<Void> prepare = CompletableFuture.runAsync(lifecycle::prepareCloseIncoming);

        // At least the channel of the message held by the gate is still draining.
        await().during(Duration.ofMillis(500)).atMost(2, TimeUnit.SECONDS).until(() -> !prepare.isDone());

        Gate.GATE.complete(null);
        await().atMost(20, TimeUnit.SECONDS).until(prepare::isDone);
        // Every delivered message was acked, and every channel was prepared, although a phase drains at most 10 at once.
        assertThat(PipelineConnector.ACKS).hasValue(Gate.RECEIVED.size());
        assertThat(PipelineConnector.PREPARED_IN).containsExactlyInAnyOrderElementsOf(channels);
    }

    private static IncomingConnectorConfiguration incomingConfig(String channel) {
        BuilderConfig config = new BuilderConfig(Map.of(
                "connector", PipelineConnector.NAME,
                "channel-name", channel,
                "graceful-shutdown", "true"));
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

    /**
     * Gates holding messages in flight: one shared gate, and one latch per payload.
     */
    static class Gate {
        static volatile CompletableFuture<Void> GATE = new CompletableFuture<>();
        static final Map<String, CountDownLatch> LATCHES = new ConcurrentHashMap<>();
        static final List<String> RECEIVED = new CopyOnWriteArrayList<>();

        static void reset() {
            GATE = new CompletableFuture<>();
            LATCHES.clear();
            RECEIVED.clear();
        }

        static CountDownLatch latch(String payload) {
            return LATCHES.computeIfAbsent(payload, p -> new CountDownLatch(1));
        }

        static void release(String payload) {
            latch(payload).countDown();
        }

        static void releaseAll() {
            GATE.complete(null);
            LATCHES.values().forEach(CountDownLatch::countDown);
        }
    }

    /**
     * Inbound and outbound connector: emits {@code count} messages (default 3) per incoming channel, counting their
     * acknowledgements, records what outgoing channels receive (acking it), and records the lifecycle calls.
     */
    @ApplicationScoped
    @Connector(PipelineConnector.NAME)
    public static class PipelineConnector implements InboundConnector, OutboundConnector {

        static final String NAME = "smallrye-pipeline";
        static final AtomicInteger ACKS = new AtomicInteger();
        static final AtomicInteger NACKS = new AtomicInteger();
        static final List<String> SINK = new CopyOnWriteArrayList<>();
        static final List<String> PREPARED_IN = new CopyOnWriteArrayList<>();
        static final List<String> PREPARED_OUT = new CopyOnWriteArrayList<>();
        static final List<String> CLOSED_IN = new CopyOnWriteArrayList<>();
        static final List<String> CLOSED_OUT = new CopyOnWriteArrayList<>();

        static void reset() {
            ACKS.set(0);
            NACKS.set(0);
            SINK.clear();
            PREPARED_IN.clear();
            PREPARED_OUT.clear();
            CLOSED_IN.clear();
            CLOSED_OUT.clear();
        }

        @Override
        public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
            String channel = config.getValue("channel-name", String.class);
            int count = config.getOptionalValue("count", Integer.class).orElse(3);
            return Multi.createFrom().range(0, count)
                    .map(i -> Message.of(channel + "-" + i,
                            () -> {
                                ACKS.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            },
                            failure -> {
                                NACKS.incrementAndGet();
                                return CompletableFuture.completedFuture(null);
                            }));
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
                    SINK.add((String) message.getPayload());
                    message.ack();
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onComplete() {
                }
            };
        }

        @Override
        public CompletionStage<Void> prepareCloseIncoming(String channel) {
            PREPARED_IN.add(channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> prepareCloseOutgoing(String channel) {
            PREPARED_OUT.add(channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> closeIncoming(String channel) {
            CLOSED_IN.add(channel);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> closeOutgoing(String channel) {
            CLOSED_OUT.add(channel);
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Holds each message until the shared gate opens, then forwards it upper-cased. The incoming message is acked
     * when the outgoing one is (manual acknowledgement, the outgoing message keeps the incoming one's ack).
     */
    @ApplicationScoped
    public static class GatedProcessor {

        @Incoming("in")
        @Outgoing("out")
        public CompletionStage<Message<String>> process(Message<String> in) {
            Gate.RECEIVED.add(in.getPayload());
            return Gate.GATE.thenApply(v -> in.withPayload(in.getPayload().toUpperCase()));
        }
    }

    /**
     * Processes messages concurrently on worker threads; each one waits for its own latch.
     */
    @ApplicationScoped
    public static class UnorderedBlockingConsumer {

        @Incoming("in")
        @Blocking(ordered = false)
        public void consume(String payload) throws InterruptedException {
            Gate.RECEIVED.add(payload);
            Gate.latch(payload).await(30, TimeUnit.SECONDS);
        }
    }

    @ApplicationScoped
    public static class TwelveChannelsConsumer {

        @Incoming("in-0")
        @Incoming("in-1")
        @Incoming("in-2")
        @Incoming("in-3")
        @Incoming("in-4")
        @Incoming("in-5")
        @Incoming("in-6")
        @Incoming("in-7")
        @Incoming("in-8")
        @Incoming("in-9")
        @Incoming("in-10")
        @Incoming("in-11")
        public CompletionStage<Void> consume(String payload) {
            Gate.RECEIVED.add(payload);
            return Gate.GATE;
        }
    }
}
