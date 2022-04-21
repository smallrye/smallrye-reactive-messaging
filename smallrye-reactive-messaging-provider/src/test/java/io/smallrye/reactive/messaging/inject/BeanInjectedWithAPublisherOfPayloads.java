package io.smallrye.reactive.messaging.inject;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.reactivestreams.Publisher;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithAPublisherOfPayloads {

    private final Publisher<String> constructor;
    @Inject
    @Channel("hello")
    private Publisher<String> field;

    @Inject
    public BeanInjectedWithAPublisherOfPayloads(@Channel("bonjour") Publisher<String> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Multi.createBy().concatenating()
                .streams(constructor, field)
                .collect().asList()
                .await().indefinitely();
    }

}
