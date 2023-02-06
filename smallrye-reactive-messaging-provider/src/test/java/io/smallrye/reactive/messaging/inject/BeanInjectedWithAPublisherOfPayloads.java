package io.smallrye.reactive.messaging.inject;

import java.util.List;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class BeanInjectedWithAPublisherOfPayloads {

    private final Flow.Publisher<String> constructor;
    @Inject
    @Channel("hello")
    private Flow.Publisher<String> field;

    @Inject
    public BeanInjectedWithAPublisherOfPayloads(@Channel("bonjour") Flow.Publisher<String> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Multi.createBy().concatenating()
                .streams(constructor, field)
                .collect().asList()
                .await().indefinitely();
    }

}
