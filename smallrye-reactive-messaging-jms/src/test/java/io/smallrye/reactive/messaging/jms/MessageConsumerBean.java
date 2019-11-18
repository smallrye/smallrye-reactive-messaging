package io.smallrye.reactive.messaging.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class MessageConsumerBean {

    private List<Integer> list = new CopyOnWriteArrayList<>();

    @Incoming("jms")
    public CompletionStage<Void> consume(ReceivedJmsMessage<Integer> v) {
        list.add(v.getPayload());
        return v.ack();
    }

    List<Integer> list() {
        return new ArrayList<>(list);
    }

}
