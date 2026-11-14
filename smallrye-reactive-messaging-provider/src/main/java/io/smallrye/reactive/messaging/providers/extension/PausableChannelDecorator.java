package io.smallrye.reactive.messaging.providers.extension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
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

@ApplicationScoped
public class PausableChannelDecorator implements PublisherDecorator, SubscriberDecorator {

    @Inject
    ChannelRegistry registry;

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
        // For incoming connector channels, the effective graceful-shutdown value was resolved by the connector.
        boolean gracefulShutdown = channelConfig
                .getOptionalValue(PausableChannelConfiguration.GRACEFUL_SHUTDOWN_PROPERTY, Boolean.class)
                .orElse(false);
        // Graceful shutdown pauses and drains the channel, so it makes the channel pausable unless configured otherwise.
        boolean pausable = channelConfig.getOptionalValue(PausableChannelConfiguration.PAUSABLE_PROPERTY, Boolean.class)
                .orElse(gracefulShutdown);
        if (!pausable) {
            return publisher;
        }
        if (channelName.stream().allMatch(name -> registry.getPausable(name) != null)) {
            // Already pausable: an emitter channel bound to an outbound connector is decorated on the emitter side
            // (publisher decorators), then on the connector side (subscriber decorators). Keep a single pause stage.
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
                .orElse(10000);

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

    @Override
    public int getPriority() {
        return PublisherDecorator.super.getPriority();
    }

    public static class PauserChannel implements PausableChannel {

        private final DemandPauser pauser;
        private final int drainTimeoutMs;
        private final AtomicInteger wip = new AtomicInteger();
        private final AtomicReference<CompletableFuture<Void>> drainFuture = new AtomicReference<>();
        private final AtomicBoolean upstreamCompleted = new AtomicBoolean();
        private final AtomicBoolean subscribed = new AtomicBoolean();
        private final Runnable onComplete;
        private final boolean tracking;

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
                onComplete.run();
                resume();
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
            return Uni.createFrom().completionStage(future);
        }

        void markSubscribed() {
            subscribed.set(true);
        }

        void markUpstreamCompleted() {
            upstreamCompleted.set(true);
            tryCompleteDrain();
        }

        @SuppressWarnings("unchecked")
        <T extends Message<?>> T trackMessage(T msg) {
            wip.incrementAndGet();
            AtomicBoolean processed = new AtomicBoolean();
            return (T) msg.withAckWithMetadata(metadata -> msg.ack(metadata).whenComplete((v, t) -> markProcessed(processed)))
                    .withNackWithMetadata(
                            (reason, metadata) -> msg.nack(reason, metadata).whenComplete((v, t) -> markProcessed(processed)));
        }

        private void markProcessed(AtomicBoolean processed) {
            if (processed.compareAndSet(false, true)) {
                if (wip.decrementAndGet() == 0) {
                    tryCompleteDrain();
                }
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
