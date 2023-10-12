package io.smallrye.reactive.messaging.providers.extension;

import static io.smallrye.reactive.messaging.providers.extension.ObservationDecorator.decorateObservation;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.SubscriberDecorator;
import io.smallrye.reactive.messaging.observation.MessageObservationCollector;

@ApplicationScoped
public class OutgoingObservationDecorator implements SubscriberDecorator {

    @Inject
    @ConfigProperty(name = "smallrye.messaging.observation.enabled", defaultValue = "true")
    boolean enabled;

    @Inject
    ChannelRegistry registry;

    @Inject
    Instance<MessageObservationCollector<?>> observationCollector;

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> multi, List<String> channelName,
            boolean isConnector) {
        String channel = channelName.isEmpty() ? null : channelName.get(0);
        boolean isEmitter = registry.getEmitterNames().contains(channel);
        if (observationCollector.isResolvable() && enabled && !isEmitter && isConnector) {
            return decorateObservation(observationCollector.get(), multi, channel, false, false);
        }
        return multi;
    }

}
