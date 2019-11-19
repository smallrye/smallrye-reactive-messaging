package io.smallrye.reactive.messaging.jms;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class RawMessageConsumerBean {

    private List<ReceivedJmsMessage<?>> messages = new CopyOnWriteArrayList<>();

    @Incoming("jms")
    public CompletionStage<Void> consume(ReceivedJmsMessage<?> v) {
        messages.add(v);
        return v.ack();
    }


    List<ReceivedJmsMessage<?>> messages() {
      return new ArrayList<>(messages);
    }

}
