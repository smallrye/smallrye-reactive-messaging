package io.smallrye.reactive.messaging.metrics;

import java.util.function.Consumer;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.PublisherDecorator;

public class MicrometerDecorator implements PublisherDecorator {

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher,
            String channelName) {
        return publisher.invoke(incrementCount(channelName));
    }

    private Consumer<Message<?>> incrementCount(String channelName) {
        Counter counter = Metrics.counter("mp.messaging.message.count", "channel", channelName);
        return m -> counter.increment();
    }
}
