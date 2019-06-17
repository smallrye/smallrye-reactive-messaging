package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class BeanInjectedWithAPublisherOfPayloads {

    private final Publisher<String> constructor;
    @Inject
    @Stream("hello")
    private Publisher<String> field;

    @Inject
    public BeanInjectedWithAPublisherOfPayloads(@Stream("bonjour") Publisher<String> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Flowable
                .concat(
                        Flowable.fromPublisher(constructor),
                        Flowable.fromPublisher(field))
                .toList()
                .blockingGet();
    }

}
