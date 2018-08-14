package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingMessagesAndProducingMessages {

  @Incoming(topic = "count")
  @Outgoing(topic = "sink")
  public Message<String> process(Message<Integer> value) {
    return Message.of(Integer.toString(value.getPayload() + 1));
  }

}
