package io.smallrye.reactive.messaging.beans;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.streams.operators.ReactiveStreams;

import mutiny.zero.flow.adapters.AdaptersToFlow;

@ApplicationScoped
public class BeanReturningASubscriberOfPayloads {

    private List<String> list = new ArrayList<>();

    @Incoming("count")
    public Flow.Subscriber<String> create() {
        return AdaptersToFlow.subscriber(ReactiveStreams.<String> builder().forEach(m -> list.add(m)).build());
    }

    public List<String> payloads() {
        return list;
    }

}
