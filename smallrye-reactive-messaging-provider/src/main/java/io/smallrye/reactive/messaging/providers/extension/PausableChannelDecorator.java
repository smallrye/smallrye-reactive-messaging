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
        boolean pausable = channelConfig.getOptionalValue(PausableChannelConfiguration.PAUSABLE_PROPERTY, Boolean.class)
                .orElse(true);
        if (!pausable) {
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
        boolean gracefulShutdown = channelConfig
                .getOptionalValue("graceful-shutdown", Boolean.class)
                .orElse(false);
        int drainTimeoutMs = channelConfig
                .getOptionalValue("graceful-shutdown.drain-timeout", Integer.class)
                .orElse(10000);

        DemandPauser pauser = new DemandPauser();
        PauserChannel pauserChannel = new PauserChannel(pauser, drainTimeoutMs);
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
            return result.onItem().transform(pauserChannel::trackMessage);
        }
        return result;
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

        public PauserChannel(DemandPauser pauser, int drainTimeoutMs) {
            this.pauser = pauser;
            this.drainTimeoutMs = drainTimeoutMs;
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

        @Override
        public Uni<Void> pauseAndDrain() {
            pause();
            if (wip.get() == 0) {
                return Uni.createFrom().voidItem();
            }
            CompletableFuture<Void> future = drainFuture.updateAndGet(f -> f != null ? f : new CompletableFuture<>());
            if (wip.get() == 0) {
                future.complete(null);
            }
            return Uni.createFrom().completionStage(future);
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
                    CompletableFuture<Void> future = drainFuture.getAndSet(null);
                    if (future != null) {
                        future.complete(null);
                    }
                }
            }
        }
    }

}
