package io.smallrye.reactive.messaging.providers.extension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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

    private final Map<String, PausableChannelConfiguration> configurations = new HashMap<>();

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher, List<String> channelName,
            boolean isConnector) {
        String channel = channelName.get(0);
        if (isConnector && configurations.containsKey(channel)) {
            PausableChannelConfiguration configuration = configurations.get(channel);
            DemandPauser pauser = new DemandPauser();
            for (String name : channelName) {
                registry.register(name, new PauserChannel(pauser));
            }
            MultiDemandPausing<? extends Message<?>> demandPausing = publisher.pauseDemand()
                    .paused(configuration.initiallyPaused())
                    .lateSubscription(configuration.lateSubscription());
            if (configuration.bufferSize() != null) {
                demandPausing = demandPausing.bufferSize(configuration.bufferSize());
            }
            if (configuration.bufferStrategy() != null) {
                demandPausing = demandPausing.bufferStrategy(getBackPressureStrategy(configuration.bufferStrategy()));
            }
            return demandPausing.using(pauser);
        }
        return publisher;
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

    public void addConfiguration(PausableChannelConfiguration configuration) {
        configurations.put(configuration.name(), configuration);
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
