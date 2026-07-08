package io.smallrye.reactive.messaging.providers.extension;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
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
                .orElse(false);
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

        DemandPauser pauser = new DemandPauser();
        for (String name : channelName) {
            registry.register(name, new PauserChannel(pauser));
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
        return demandPausing.using(pauser);
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

        public PauserChannel(DemandPauser pauser) {
            this.pauser = pauser;
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
    }

}
