package io.smallrye.reactive.messaging.decorator;

import java.util.concurrent.atomic.AtomicInteger;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.smallrye.reactive.messaging.PublisherDecorator;

@ApplicationScoped
public class CountingDecorator implements PublisherDecorator {

    private AtomicInteger messsageCount = new AtomicInteger(0);

    @Override
    public PublisherBuilder<? extends Message> decorate(PublisherBuilder<? extends Message> publisher,
            String channelName) {
        return publisher.peek(m -> messsageCount.incrementAndGet());
    }

    public int getMesssageCount() {
        return messsageCount.get();
    }

}
