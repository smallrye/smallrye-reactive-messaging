package io.smallrye.reactive.messaging.beans;

import java.util.concurrent.Flow.Subscriber;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanReturningASubscriberOfMessagesButDiscarding {

    @Incoming("subscriber")
    public Subscriber<Message<String>> create() {
        return AdaptersToFlow.subscriber(ReactiveStreams.<Message<String>> builder()
                .ignore().build());
    }

}
