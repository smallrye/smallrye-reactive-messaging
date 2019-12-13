package io.smallrye.reactive.messaging.metrics;

import java.util.function.Consumer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.Tag;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.smallrye.reactive.messaging.PublisherDecorator;

@ApplicationScoped
public class MetricDecorator implements PublisherDecorator {

    private MetricRegistry registry;

    @Inject
    private void setMetricRegistry(Instance<MetricRegistry> registryInstance) {
        if (registryInstance.isResolvable()) {
            registry = registryInstance.get();
        }
    }

    @Override
    public PublisherBuilder<? extends Message> decorate(PublisherBuilder<? extends Message> publisher,
            String channelName) {
        if (registry != null) {
            return publisher.peek(incrementCount(channelName));
        } else {
            return publisher;
        }
    }

    private Consumer<Message> incrementCount(String channelName) {
        Counter counter = registry.counter("mp.messaging.message.count", new Tag("channel", channelName));
        return (m) -> counter.inc();
    }

}
