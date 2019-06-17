package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class BeanInjectedWithAPublisherOfMessages {

    private final Publisher<Message<String>> constructor;

    @Inject
    @Stream("hello")
    private Publisher<Message<String>> field;

    @Inject
    public BeanInjectedWithAPublisherOfMessages(@Stream("bonjour") Publisher<Message<String>> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Flowable
                .concat(
                        Flowable.fromPublisher(constructor),
                        Flowable.fromPublisher(field))
                .map(Message::getPayload)
                .toList()
                .blockingGet();
    }

}
