package io.smallrye.reactive.messaging.inject;

import java.util.List;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithARSPublisherOfMessages {

    private final Flow.Publisher<Message<String>> constructor;

    @Inject
    @Channel("hello")
    private Flow.Publisher<Message<String>> field;

    @Inject
    public BeanInjectedWithARSPublisherOfMessages(@Channel("bonjour") Flow.Publisher<Message<String>> constructor) {
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
