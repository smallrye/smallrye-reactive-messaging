package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfMessagesUsingStream {

    private final PublisherBuilder<Message<String>> constructor;
    @Inject
    @Stream("hello")
    private PublisherBuilder<Message<String>> field;

    @Inject
    public BeanInjectedWithAPublisherBuilderOfMessagesUsingStream(
            @Stream("bonjour") PublisherBuilder<Message<String>> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Flowable.fromPublisher(
                ReactiveStreams.concat(constructor, field).map(Message::getPayload).buildRs())
                .toList()
                .blockingGet();
    }

}
