package io.smallrye.reactive.messaging.beans;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BeanConsumingItemsAndProducingItems {

  @Incoming("count")
  @Outgoing("sink")
  public String process(int value) {
    return Integer.toString(value + 1);
  }

}
