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

    @Inject
    private Instance<MetricRegistry> registryInstance;

    @Override
    public PublisherBuilder<? extends Message> decoratePublisher(PublisherBuilder<? extends Message> publisher,
            String channelName) {
        if (registryInstance.isResolvable()) {
            return publisher.peek(incrementCount(channelName));
        } else {
            return publisher;
        }
    }

    private <T extends Message> Consumer<T> incrementCount(String channelName) {
        MetricRegistry registry = registryInstance.get();
        Counter counter = registry.counter("mp.messaging.message.count", new Tag("channel", channelName));
        return (m) -> counter.inc();
    }

}
