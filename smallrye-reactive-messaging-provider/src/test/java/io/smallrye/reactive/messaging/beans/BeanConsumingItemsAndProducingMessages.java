package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingItemsAndProducingMessages {

  @Incoming("count")
  @Outgoing("sink")
  public Message<String> process(int value) {
    return Message.of(Integer.toString(value + 1));
  }

}
