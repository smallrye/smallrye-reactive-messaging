package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfMessages {

    private final PublisherBuilder<Message<String>> constructor;
    @Inject
    @Channel("hello")
    private PublisherBuilder<Message<String>> field;

    @Inject
    public BeanInjectedWithAPublisherBuilderOfMessages(@Channel("bonjour") PublisherBuilder<Message<String>> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Flowable.fromPublisher(
                ReactiveStreams.concat(constructor, field).map(Message::getPayload).buildRs())
                .toList()
                .blockingGet();
    }

}
