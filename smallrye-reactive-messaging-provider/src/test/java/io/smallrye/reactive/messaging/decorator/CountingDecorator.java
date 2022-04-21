package io.smallrye.reactive.messaging.decorator;

import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.providers.PublisherDecorator;

@ApplicationScoped
public class CountingDecorator implements PublisherDecorator {

    private final AtomicInteger messageCount = new AtomicInteger(0);

    @Override
    public Multi<? extends Message<?>> decorate(Multi<? extends Message<?>> publisher,
            String channelName) {
        return publisher.invoke(m -> messageCount.incrementAndGet());
    }

    public int getMessageCount() {
        return messageCount.get();
    }

}
