package io.smallrye.reactive.messaging.providers.extension;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.MultiDemandPausing;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.DemandPauser;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.PausableChannel;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;
import io.smallrye.reactive.messaging.PausableChannelConfiguration.PausableBufferStrategy;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.connector.InboundConnector;

@ApplicationScoped
public class PausableChannelDecorator implements PublisherDecorator, SubscriberDecorator {

    /**
     * The drain timeout, in milliseconds, when neither the channel, its connector nor
     * {@link PausableChannelConfiguration#GRACEFUL_SHUTDOWN_DEFAULT_DRAIN_TIMEOUT_PROPERTY} configure one.
     */
    public static final int DEFAULT_DRAIN_TIMEOUT_MS = 10000;

    @Inject
    ChannelRegistry registry;

    @Inject
    Instance<Config> config;

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            boolean isConnector) {
        return publisher;
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            Config channelConfig) {
        if (channelConfig == null) {
            return publisher;
        }
        // Incoming connector channels arrive with the value already resolved with their connector (see
        // isGracefulShutdownEnabled), which this call returns unchanged; outgoing channels are resolved here.
        boolean gracefulShutdown = isGracefulShutdownEnabled(channelConfig, globalConfig(), null);
        // Graceful shutdown pauses and drains the channel, so it makes the channel pausable unless configured otherwise.
        boolean pausable = channelConfig.getOptionalValue(PausableChannelConfiguration.PAUSABLE_PROPERTY, Boolean.class)
                .orElse(gracefulShutdown);
        if (!pausable) {
            return publisher;
        }
        if (channelName.stream().allMatch(name -> registry.getPausable(name) != null)) {
            // Already pausable: keep a single pause stage per channel. Emitter channels get theirs on the connector
            // side (subscriber decorators), as the emitter side is decorated with a null configuration.
            return publisher;
        }
        boolean initiallyPaused = channelConfig
                .getOptionalValue(PausableChannelConfiguration.INITIALLY_PAUSED_PROPERTY, Boolean.class)
                .or(() -> channelConfig.getOptionalValue(PausableChannelConfiguration.PAUSED_PROPERTY, Boolean.class))
                .orElse(false);
        boolean lateSubscription = channelConfig
                .getOptionalValue(PausableChannelConfiguration.LATE_SUBSCRIPTION_PROPERTY, Boolean.class)
                .orElse(false);
        Integer bufferSize = channelConfig
                .getOptionalValue(PausableChannelConfiguration.BUFFER_SIZE_PROPERTY, Integer.class)
                .orElse(null);
        PausableBufferStrategy bufferStrategy = channelConfig
                .getOptionalValue(PausableChannelConfiguration.BUFFER_STRATEGY_PROPERTY, PausableBufferStrategy.class)
                .orElse(null);
        int drainTimeoutMs = channelConfig
                .getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT_PROPERTY, Integer.class)
                .orElseGet(() -> defaultDrainTimeoutMs(globalConfig()));

        Runnable onComplete = null;
        String emitterChannel = channelName.get(0);
        if (!registry.getEmitters(emitterChannel).isEmpty()) {
            // Resolved when draining, so every emitter registered for the channel is completed.
            onComplete = () -> completeEmitters(registry.getEmitters(emitterChannel));
        }

        DemandPauser pauser = new DemandPauser();
        PauserChannel pauserChannel = new PauserChannel(pauser, drainTimeoutMs, onComplete, gracefulShutdown);
        for (String name : channelName) {
            registry.register(name, pauserChannel);
        }
        MultiDemandPausing<? extends Message<?>> demandPausing = publisher.pauseDemand()
                .paused(initiallyPaused)
                .lateSubscription(lateSubscription);
        if (bufferSize != null) {
            demandPausing = demandPausing.bufferSize(bufferSize);
        }
        if (bufferStrategy != null) {
            demandPausing = demandPausing.bufferStrategy(getBackPressureStrategy(bufferStrategy));
        }
        Multi<? extends Message<?>> result = demandPausing.using(pauser);
        if (gracefulShutdown) {
            return result.onItem().transform(pauserChannel::trackMessage)
                    .onSubscription().invoke(pauserChannel::markSubscribed)
                    .onTermination().invoke(pauserChannel::markUpstreamCompleted);
        }
        return result;
    }

    private Config globalConfig() {
        if (config == null || !config.isResolvable()) {
            return null;
        }
        try {
            return config.get();
        } catch (RuntimeException e) {
            // Released during shutdown: use the default.
            return null;
        }
    }

    /**
     * Resolves whether a channel tracks and drains its in-flight messages on shutdown. This is the only place where
     * the rule is implemented, for incoming and outgoing channels:
     * <ol>
     * <li>a {@code graceful-shutdown} property set on the channel or on its connector always applies;</li>
     * <li>otherwise, when {@link PausableChannelConfiguration#GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY} is
     * {@code false}, graceful shutdown is disabled;</li>
     * <li>otherwise, an incoming connector channel takes its connector's default
     * ({@link InboundConnector#isGracefulShutdownEnabled(Config)}), and an outgoing channel is enabled.</li>
     * </ol>
     * The connector is asked in the first and last cases only, so that it reads an explicit value and declares its
     * own default, but cannot override a disabled global default.
     *
     * @param channelConfig the channel configuration, which falls back to the connector-level properties
     * @param globalConfig the application configuration, may be {@code null}
     * @param connector the connector of an incoming connector channel, {@code null} otherwise
     * @return whether in-flight messages are tracked and drained on shutdown
     */
    public static boolean isGracefulShutdownEnabled(Config channelConfig, Config globalConfig,
            InboundConnector connector) {
        Optional<Boolean> explicit = channelConfig
                .getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, Boolean.class);
        if (explicit.isEmpty() && !isEnabledByDefault(globalConfig)) {
            return false;
        }
        if (connector != null) {
            return connector.isGracefulShutdownEnabled(channelConfig);
        }
        return explicit.orElse(true);
    }

    /**
     * @param config the application configuration, may be {@code null}
     * @return whether graceful shutdown is enabled for the channels that do not configure it
     *         ({@link PausableChannelConfiguration#GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY}, {@code true} by default)
     */
    public static boolean isEnabledByDefault(Config config) {
        if (config == null) {
            return true;
        }
        return config.getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_DEFAULT_ENABLED_PROPERTY, Boolean.class)
                .orElse(true);
    }

    /**
     * @param config the application configuration, may be {@code null}
     * @return the drain timeout, in milliseconds, of the channels that configure none
     *         ({@link PausableChannelConfiguration#GRACEFUL_SHUTDOWN_DEFAULT_DRAIN_TIMEOUT_PROPERTY},
     *         {@value #DEFAULT_DRAIN_TIMEOUT_MS} by default)
     */
    public static int defaultDrainTimeoutMs(Config config) {
        if (config == null) {
            return DEFAULT_DRAIN_TIMEOUT_MS;
        }
        return config
                .getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_DEFAULT_DRAIN_TIMEOUT_PROPERTY, Integer.class)
                .orElse(DEFAULT_DRAIN_TIMEOUT_MS);
    }

    /**
     * Stops tracking the in-flight messages of a pausable channel created by this decorator, including each part of
     * a {@link CompositePausableChannel} (channels with {@code concurrency}).
     *
     * @param channel the pausable channel, may be {@code null}
     * @return {@code true} if tracking was enabled on at least one channel and is now disabled
     */
    public static boolean disableTracking(PausableChannel channel) {
        if (channel instanceof PauserChannel pauser) {
            boolean wasTracking = pauser.isTracking();
            pauser.disableTracking();
            return wasTracking;
        }
        if (channel instanceof CompositePausableChannel composite) {
            boolean disabled = false;
            for (PausableChannel part : composite.getParts()) {
                disabled |= disableTracking(part);
            }
            return disabled;
        }
        return false;
    }

    static void completeEmitters(List<MessagePublisherProvider<?>> emitters) {
        RuntimeException failure = null;
        for (MessagePublisherProvider<?> emitter : emitters) {
            try {
                emitter.complete();
            } catch (RuntimeException e) {
                // Keep completing the other emitters, report the first failure afterwards.
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    BackPressureStrategy getBackPressureStrategy(PausableBufferStrategy pausableBufferStrategy) {
        return switch (pausableBufferStrategy) {
            case DROP -> BackPressureStrategy.DROP;
            case IGNORE -> BackPressureStrategy.IGNORE;
            default -> BackPressureStrategy.BUFFER;
        };
    }

    /**
     * After the decorators with the default priority, such as the observation and metrics decorators, so that on
     * incoming channels the tracked message is the one delivered to the application, and before the outgoing
     * interceptors (2000). Sorting is by priority only: an equal priority would leave the order to the container.
     */
    @Override
    public int getPriority() {
        return 1500;
    }

    public static class PauserChannel implements PausableChannel {

        private final DemandPauser pauser;
        private final int drainTimeoutMs;
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<CompletableFuture<Void>> drainFuture = new AtomicReference<>();
        private final AtomicBoolean upstreamCompleted = new AtomicBoolean();
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final Runnable onComplete;
        private volatile boolean tracking;
        // Shared by the tracked messages of the channel, so tracking a message allocates nothing else.
        private final Runnable onMessageProcessed = this::messageProcessed;

        public PauserChannel(DemandPauser pauser, int drainTimeoutMs, Runnable onComplete) {
            this(pauser, drainTimeoutMs, onComplete, true);
        }

        /**
         * @param pauser the demand pauser of the channel
         * @param drainTimeoutMs the drain timeout, in milliseconds
         * @param onComplete completes the emitters of the channel, {@code null} if the channel is not fed by emitters
         * @param tracking whether in-flight messages are tracked ({@code graceful-shutdown} enabled). Without tracking,
         *        {@link #pauseAndDrain()} does not wait.
         */
        public PauserChannel(DemandPauser pauser, int drainTimeoutMs, Runnable onComplete, boolean tracking) {
            this.pauser = pauser;
            this.drainTimeoutMs = drainTimeoutMs;
            this.onComplete = onComplete;
            this.tracking = tracking;
        }

        @Override
        public boolean isPaused() {
            return pauser.isPaused();
        }

        @Override
        public void pause() {
            pauser.pause();
        }

        @Override
        public void resume() {
            pauser.resume();
        }

        @Override
        public void clearBuffer() {
            pauser.clearBuffer();
        }

        @Override
        public Duration getDrainTimeout() {
            return Duration.ofMillis(drainTimeoutMs);
        }

        /**
         * Stops tracking the in-flight messages: {@link #pauseAndDrain()} then pauses (or flushes the emitters) without
         * waiting for the messages to be processed. Used when the channel's consumers never acknowledge messages
         * ({@code @Acknowledgment(NONE)}), as the drain would always wait for its timeout. Must be called before the
         * channel is subscribed: messages tracked before stay counted.
         */
        public void disableTracking() {
            tracking = false;
        }

        /**
         * @return whether in-flight messages are tracked
         */
        public boolean isTracking() {
            return tracking;
        }

        /**
         * @return the number of messages delivered downstream and not yet acknowledged or negatively acknowledged,
         *         always {@code 0} when in-flight messages are not tracked
         */
        public int getInFlightCount() {
            return wip.get();
        }

        @Override
        public Uni<Void> pauseAndDrain() {
            if (onComplete != null) {
                // Emitter channel: complete the emitters so buffered messages are flushed, then wait until the
                // completion has gone through the stream (all messages emitted) and every message is processed.
                try {
                    onComplete.run();
                } finally {
                    // Even if an emitter failed to complete: the messages buffered while paused are delivered.
                    resume();
                }
                if (!tracking || !subscribed.get()) {
                    // Nothing tracked, or nobody consumes the stream so the completion will never be observed.
                    return Uni.createFrom().voidItem();
                }
            } else {
                // Incoming channel: stop the flow, then wait for the messages already delivered.
                pause();
                if (!tracking) {
                    return Uni.createFrom().voidItem();
                }
                upstreamCompleted.set(true);
            }
            return waitForDrain();
        }

        private Uni<Void> waitForDrain() {
            CompletableFuture<Void> future = drainFuture.updateAndGet(f -> f != null ? f : new CompletableFuture<>());
            // The conditions may have been reached before the future was installed: re-check now.
            // Every state change (wip reaching 0, upstream completion) calls tryCompleteDrain after updating its
            // state, so whichever happens last completes the future.
            tryCompleteDrain();
            // A copy: cancelling the returned Uni, as a drain timeout does, cancels the stage it was created from,
            // and the shared future must stay usable for the next drain.
            return Uni.createFrom().completionStage(future.copy());
        }

        void markSubscribed() {
            subscribed.set(true);
        }

        void markUpstreamCompleted() {
            upstreamCompleted.set(true);
            tryCompleteDrain();
        }

        /**
         * Tracks the message as in flight until it is acknowledged or negatively acknowledged.
         * <p>
         * Internal: public so that connector tests can build the messages a tracking channel delivers.
         *
         * @param msg the message to track
         * @return the tracked message, which does not keep the type of the given message, or the message itself when
         *         tracking is disabled
         */
        public Message<?> trackMessage(Message<?> msg) {
            if (!tracking) {
                return msg;
            }
            wip.incrementAndGet();
            return new TrackedMessage<>(msg, onMessageProcessed);
        }

        private void messageProcessed() {
            if (wip.decrementAndGet() == 0) {
                tryCompleteDrain();
            }
        }

        private void tryCompleteDrain() {
            if (upstreamCompleted.get() && wip.get() == 0) {
                CompletableFuture<Void> future = drainFuture.getAndSet(null);
                if (future != null) {
                    future.complete(null);
                }
            }
        }
    }

}
