package io.smallrye.reactive.messaging.metrics;

import java.util.function.Consumer;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.MetricRegistry.Type;
import org.eclipse.microprofile.metrics.Tag;
import org.eclipse.microprofile.metrics.annotation.RegistryType;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;

public class MetricDecorator implements PublisherDecorator {

    private MetricRegistry registry;

    @Inject
    private void setMetricRegistry(@RegistryType(type = Type.BASE) Instance<MetricRegistry> registryInstance) {
        if (registryInstance.isResolvable()) {
            registry = registryInstance.get();
        }
    }

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher,
            String channelName) {
        if (registry != null) {
            return publisher.invoke(incrementCount(channelName));
        } else {
            return publisher;
        }
    }

    private Consumer<Message<?>> incrementCount(String channelName) {
        Counter counter = registry.counter("mp.messaging.message.count", new Tag("channel", channelName));
        return m -> counter.inc();
    }

}
