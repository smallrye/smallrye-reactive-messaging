package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Stream;

@ApplicationScoped
public class BeanInjectedWithAFlowableOfPayloadsUsingStream {

    private final Flowable<String> constructor;
    @Inject
    @Stream("hello")
    private Flowable<String> field;

    @Inject
    public BeanInjectedWithAFlowableOfPayloadsUsingStream(@Stream("bonjour") Flowable<String> constructor) {
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
