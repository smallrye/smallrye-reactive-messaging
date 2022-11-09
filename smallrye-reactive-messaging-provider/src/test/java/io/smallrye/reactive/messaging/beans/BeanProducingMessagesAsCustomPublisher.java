package io.smallrye.reactive.messaging.beans;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.operators.AbstractMulti;
import io.smallrye.mutiny.subscription.MultiSubscriber;

@ApplicationScoped
public class BeanProducingMessagesAsCustomPublisher {

    @Outgoing("sink")
    public MyCustomPublisher<Message<String>> publisher() {
        return Multi.createFrom().range(1, 11)
                .flatMap(i -> Multi.createFrom().items(i, i))
                .map(i -> Integer.toString(i))
                .map(Message::of)
                .convert().with(MyCustomPublisher::new);
    }

    static class MyCustomPublisher<T> extends AbstractMulti<T> {

        private final Multi<T> multi;

        public MyCustomPublisher(Multi<T> multi) {
            this.multi = multi;
        }

        @Override
        public void subscribe(MultiSubscriber<? super T> subscriber) {
            multi.subscribe(subscriber);
        }
    }

}
