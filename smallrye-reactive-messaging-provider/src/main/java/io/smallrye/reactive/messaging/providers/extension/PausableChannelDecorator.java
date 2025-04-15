package io.smallrye.reactive.messaging.providers.extension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.PausableChannelConfiguration;
import io.smallrye.reactive.messaging.PublisherDecorator;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.providers.helpers.PausableMulti;

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
            PausableMulti<? extends Message<?>> pausable = new PausableMulti<>(publisher, configuration);
            for (String name : channelName) {
                registry.register(name, pausable);
            }
            return pausable;
        }
        return publisher;
    }

    @Override
    public int getPriority() {
        return PublisherDecorator.super.getPriority();
    }

    public void addConfiguration(PausableChannelConfiguration configuration) {
        configurations.put(configuration.name(), configuration);
    }

}
