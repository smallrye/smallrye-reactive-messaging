package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.streams.operators.PublisherBuilder;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class BeanInjectedWithAPublisherBuilderOfPayloads {

    private final PublisherBuilder<String> constructor;
    @Inject
    @Stream("hello")
    private PublisherBuilder<String> field;

    @Inject
    public BeanInjectedWithAPublisherBuilderOfPayloads(@Stream("bonjour") PublisherBuilder<String> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Flowable.fromPublisher(
                ReactiveStreams.concat(constructor, field).buildRs())
                .toList()
                .blockingGet();
    }

}
