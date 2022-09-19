package io.smallrye.reactive.messaging.inject;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfMessages {

    private final PublisherBuilder<Message<String>> constructor;
    @Inject
    @Channel("hello")
    private PublisherBuilder<Message<String>> field;

    @Inject
    public BeanInjectedWithAPublisherBuilderOfMessages(
            @Channel("bonjour") PublisherBuilder<Message<String>> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Multi.createBy().concatenating()
                .streams(constructor.buildRs(), field.buildRs())
                .map(Message::getPayload)
                .collect().asList()
                .await().indefinitely();
    }

}
