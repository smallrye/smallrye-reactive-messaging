package io.smallrye.reactive.messaging.json;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PersonProducerBean {

    @Outgoing("queue-one")
    public PublisherBuilder<Person> producer() {
        return ReactiveStreams.of(
                new Person("bob", 20),
                new Person("tom", 18),
                new Person("phil", 30));
    }

}
