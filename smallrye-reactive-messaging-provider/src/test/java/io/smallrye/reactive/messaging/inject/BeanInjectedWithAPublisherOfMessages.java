package io.smallrye.reactive.messaging.inject;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithAPublisherOfMessages {

    private final Publisher<Message<String>> constructor;

    @Inject
    @Channel("hello")
    private Publisher<Message<String>> field;

    @Inject
    public BeanInjectedWithAPublisherOfMessages(@Channel("bonjour") Publisher<Message<String>> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Multi.createBy().concatenating()
                .streams(constructor, field)
                .map(Message::getPayload)
                .collect().asList()
                .await().indefinitely();
    }

}
