package io.smallrye.reactive.messaging.inject;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.reactivex.Flowable;

@ApplicationScoped
public class BeanInjectedWithAFlowableOfMessages {

    private final Flowable<Message<String>> constructor;
    @Inject
    @Channel("hello")
    private Flowable<Message<String>> field;

    @Inject
    public BeanInjectedWithAFlowableOfMessages(@Channel("bonjour") Flowable<Message<String>> constructor) {
        this.constructor = constructor;
    }

    public List<String> consume() {
        return Flowable
                .concat(
                        Flowable.fromPublisher(constructor),
                        Flowable.fromPublisher(field))
                .map(Message::getPayload)
                .toList()
                .blockingGet();
    }

}
