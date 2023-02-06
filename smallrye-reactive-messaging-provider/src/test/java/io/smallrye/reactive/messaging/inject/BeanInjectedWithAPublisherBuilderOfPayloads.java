package io.smallrye.reactive.messaging.inject;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;

import io.smallrye.mutiny.Multi;
import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfPayloads {

    private final PublisherBuilder<String> constructor;
    @Inject
    @Channel("hello")
    private PublisherBuilder<String> field;

    @Inject
    public BeanInjectedWithAPublisherBuilderOfPayloads(@Channel("bonjour") PublisherBuilder<String> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Multi.createBy().concatenating()
                .streams(AdaptersToFlow.publisher(constructor.buildRs()), AdaptersToFlow.publisher(field.buildRs()))
                .collect().asList()
                .await().indefinitely();
    }

}
