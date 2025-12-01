package io.smallrye.reactive.messaging.kafka;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.Producer;

@ApplicationScoped
public class KafkaCDIEvents {
    @Inject
    Event<Consumer<?, ?>> consumerEvent;

    @Inject
    Event<ShareConsumer<?, ?>> shareConsumerEvent;

    @Inject
    Event<Producer<?, ?>> producerEvent;

    public Event<Consumer<?, ?>> consumer() {
        return consumerEvent;
    }

    public Event<ShareConsumer<?, ?>> shareConsumer() {
        return shareConsumerEvent;
    }

    public Event<Producer<?, ?>> producer() {
        return producerEvent;
    }

    public KafkaCDIEvents() {
    }
}
