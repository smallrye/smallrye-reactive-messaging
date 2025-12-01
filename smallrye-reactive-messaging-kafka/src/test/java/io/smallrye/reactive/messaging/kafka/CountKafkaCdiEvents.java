package io.smallrye.reactive.messaging.kafka;

import java.lang.annotation.Annotation;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;

import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.NotificationOptions;
import jakarta.enterprise.util.TypeLiteral;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.Producer;

public class CountKafkaCdiEvents extends KafkaCDIEvents {
    public static final KafkaCDIEvents noCdiEvents = new CountKafkaCdiEvents();

    public final LongAdder firedConsumerEvents = new LongAdder();
    public final LongAdder firedShareConsumerEvents = new LongAdder();
    public final LongAdder firedProducerEvents = new LongAdder();

    public CountKafkaCdiEvents() {
        this.consumerEvent = new Event<Consumer<?, ?>>() {
            @Override
            public void fire(Consumer<?, ?> event) {
                firedConsumerEvents.increment();
            }

            @Override
            public <U extends Consumer<?, ?>> CompletionStage<U> fireAsync(U event) {
                firedConsumerEvents.increment();
                return null;
            }

            @Override
            public <U extends Consumer<?, ?>> CompletionStage<U> fireAsync(U event, NotificationOptions options) {
                firedConsumerEvents.increment();
                return null;
            }

            @Override
            public Event<Consumer<?, ?>> select(Annotation... qualifiers) {
                return null;
            }

            @Override
            public <U extends Consumer<?, ?>> Event<U> select(Class<U> subtype, Annotation... qualifiers) {
                return null;
            }

            @Override
            public <U extends Consumer<?, ?>> Event<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
                return null;
            }
        };

        this.shareConsumerEvent = new Event<ShareConsumer<?, ?>>() {
            @Override
            public void fire(ShareConsumer<?, ?> event) {
                firedShareConsumerEvents.increment();
            }

            @Override
            public <U extends ShareConsumer<?, ?>> CompletionStage<U> fireAsync(U event) {
                firedShareConsumerEvents.increment();
                return null;
            }

            @Override
            public <U extends ShareConsumer<?, ?>> CompletionStage<U> fireAsync(U event, NotificationOptions options) {
                firedShareConsumerEvents.increment();
                return null;
            }

            @Override
            public Event<ShareConsumer<?, ?>> select(Annotation... qualifiers) {
                return null;
            }

            @Override
            public <U extends ShareConsumer<?, ?>> Event<U> select(Class<U> subtype, Annotation... qualifiers) {
                return null;
            }

            @Override
            public <U extends ShareConsumer<?, ?>> Event<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
                return null;
            }
        };

        this.producerEvent = new Event<Producer<?, ?>>() {
            @Override
            public void fire(Producer<?, ?> event) {
                firedProducerEvents.increment();
            }

            @Override
            public <U extends Producer<?, ?>> CompletionStage<U> fireAsync(U event) {
                firedProducerEvents.increment();
                return null;
            }

            @Override
            public <U extends Producer<?, ?>> CompletionStage<U> fireAsync(U event, NotificationOptions options) {
                firedProducerEvents.increment();
                return null;
            }

            @Override
            public Event<Producer<?, ?>> select(Annotation... qualifiers) {
                return null;
            }

            @Override
            public <U extends Producer<?, ?>> Event<U> select(Class<U> subtype, Annotation... qualifiers) {
                return null;
            }

            @Override
            public <U extends Producer<?, ?>> Event<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
                return null;
            }
        };
    }
}
