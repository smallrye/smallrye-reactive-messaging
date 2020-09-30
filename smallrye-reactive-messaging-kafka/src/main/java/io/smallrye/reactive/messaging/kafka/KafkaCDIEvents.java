package io.smallrye.reactive.messaging.kafka;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

@ApplicationScoped
public class KafkaCDIEvents {
    @Inject
    Event<Consumer<?, ?>> consumerEvent;

    @Inject
    Event<Producer<?, ?>> producerEvent;

    public Event<Consumer<?, ?>> consumer() {
        return consumerEvent;
    }

    public Event<Producer<?, ?>> producer() {
        return producerEvent;
    }

    public KafkaCDIEvents() {
    }
}
