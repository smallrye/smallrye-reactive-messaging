package io.smallrye.reactive.messaging.providers.metrics;

import java.util.function.Consumer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.PublisherDecorator;

@ApplicationScoped
public class MicrometerDecorator implements PublisherDecorator {

    @Inject
    @ConfigProperty(name = "smallrye.messaging.metrics.micrometer.enabled", defaultValue = "true")
    boolean enabled;

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher,
            String channelName) {
        if (enabled) {
            return publisher.invoke(incrementCount(channelName));
        } else {
            return publisher;
        }
    }

    private Consumer<Message<?>> incrementCount(String channelName) {
        Counter counter = Metrics.counter("mp.messaging.message.count", "channel", channelName);
        return m -> counter.increment();
    }
}
